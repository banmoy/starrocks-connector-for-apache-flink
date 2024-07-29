/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.data.load.stream.groupcommit;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.data.load.stream.TransactionStatus;
import com.starrocks.data.load.stream.groupcommit.thrift.ThriftClient;
import com.starrocks.data.load.stream.groupcommit.thrift.ThriftClientPool;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LabelMetaService extends SharedService<LabelMetaService.LabelMetaConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(LabelMetaService.class);

    private static final LabelMetaService INSTANCE = new LabelMetaService();

    private final ConcurrentHashMap<TableId, TableLabelHolder> labelHolderMap;

    private String[] hosts;
    private String user;
    private String password;
    private long checkLabelIntervalMs;
    private long checkLabelTimeoutMs;
    private ScheduledExecutorService scheduledExecutorService;
    private final ObjectMapper objectMapper;
    private CloseableHttpClient httpClient;

    private ThriftClientPool thriftClientPool;

    public LabelMetaService() {
        this.objectMapper = new ObjectMapper();
        // StreamLoadResponseBody does not contain all fields returned by StarRocks
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // filed names in StreamLoadResponseBody are case-insensitive
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        this.labelHolderMap = new ConcurrentHashMap<>();
    }

    public static LabelMetaService getInstance() {
        return INSTANCE;
    }

    @Override
    protected void init(LabelMetaConfig labelMetaConfig) throws Exception {
        StreamLoadProperties properties = labelMetaConfig.properties;
        this.hosts = properties.getLoadUrls();
        this.user = properties.getUsername();
        this.password = properties.getPassword();
        this.checkLabelIntervalMs = properties.getCheckLabelIntervalMs();
        this.checkLabelTimeoutMs = properties.getCheckLabelTimeoutMs();
        int threadPoolSize = 10;
        URL url = new URL(hosts[0]);
        this.thriftClientPool =
            new ThriftClientPool(url.getHost(), 9020, threadPoolSize);
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(
                threadPoolSize,
                r -> {
                    Thread thread = new Thread(null, r, "LabelMetaService-" + UUID.randomUUID());
                    thread.setDaemon(true);
                    return thread;
                });

//        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
//        cm.setDefaultMaxPerRoute(threadPoolSize);
//        cm.setMaxTotal(threadPoolSize);
//        httpClient = HttpClients.custom()
//                .setConnectionManager(cm)
//                .build();

        this.scheduledExecutorService.schedule(this::cleanUselessLabels, 30, TimeUnit.SECONDS);
        LOG.info("Init label meta service, checkLabelIntervalMs: {}, checkLabelTimeoutMs: {}, threadPoolSize: {}",
                checkLabelIntervalMs, checkLabelTimeoutMs, threadPoolSize);
    }

    @Override
    protected void reset() {
        if (this.scheduledExecutorService != null) {
            this.scheduledExecutorService.shutdownNow();
            this.scheduledExecutorService = null;
        }
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (Exception e) {
                LOG.error("Failed to close client", e);
            }
            this.httpClient = null;
        }
        if (thriftClientPool != null) {
            thriftClientPool.close();
        }
        labelHolderMap.clear();
        LOG.info("Reset label meta service");
    }

    public CompletableFuture<LabelMeta>
    getLabelFinalStatusAsync(TableId tableId, String label,
                             long expectFinishTimeMs) {
      LabelMeta labelMeta =
          labelHolderMap
              .computeIfAbsent(tableId, key -> new TableLabelHolder(tableId))
              .addLabel(label, expectFinishTimeMs, checkLabelIntervalMs);
      if (labelMeta.isScheduled.compareAndSet(false, true)) {
        labelMeta.firstDelay =
            expectFinishTimeMs > 0
                ? Math.max(0, expectFinishTimeMs - System.currentTimeMillis()) + 50
                : checkLabelIntervalMs;
        scheduledExecutorService.schedule(()
                                              -> checkLabelStateByThrift(labelMeta),
                                          labelMeta.firstDelay,
                                          TimeUnit.MILLISECONDS);
        LOG.info(
            "Schedule to get label state, db: {}, table: {}, label: {}, delay: {}ms",
            tableId.db, tableId.table, label, labelMeta.firstDelay);
      }
      return labelMeta.future;
    }

    private void checkLabelStateByThrift(LabelMeta labelMeta) {
        labelMeta.numRetries += 1;
        ThriftClient client = null;
        try {
            long startTs = System.currentTimeMillis();
            client = thriftClientPool.borrowClient();
            long getClientTs = System.currentTimeMillis();
            labelMeta.getClientCostMs += (getClientTs - startTs);
            TransactionStatus status = LabelUtils.getLabelStatus(client, labelMeta.tableId.db, labelMeta.label);
            labelMeta.getLabelCostMs += (System.currentTimeMillis() - getClientTs);
            if (TransactionStatus.isFinalStatus(status)) {
                labelMeta.transactionStatus = status;
                labelMeta.finishTimeMs = System.currentTimeMillis();
                labelMeta.future.complete(labelMeta);
                long costMs = labelMeta.finishTimeMs - labelMeta.createTimeMs;
                LOG.info(
                        "Get final label state, db: {}, table: {}, label: {}, cost: {}ms, retries: {}, status: {}, client: {}",
                        labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label,
                        costMs, labelMeta.numRetries, status, client.getId());
                return;
            }
            LOG.info(
                    "Label is not in final status, db: {}, table: {}, label: {}, status: {}, client: {}",
                    labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label, status, client.getId());
        } catch (Exception e) {
            LOG.error("Failed to get label state, db: {}, table: {}, label: {}, client: {}",
                    labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label,
                    client == null ? null : client.getId(), e);
            if (client != null && e instanceof TTransportException) {
                thriftClientPool.removeClient(client);
                client = null;
            }
        } finally {
            if (client != null) {
                thriftClientPool.returnClient(client);
            }
        }
        TableLabelHolder holder = labelHolderMap.get(labelMeta.tableId);
        if (holder == null || holder.getLabel(labelMeta.label) != labelMeta) {
            labelMeta.future.completeExceptionally(new RuntimeException("Label is discarded"));
            LOG.error("Failed to retry to get label state because label is discarded, db: {}, table: {}, label: {}",
                    labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label);
            return;
        }

        if (System.currentTimeMillis() - labelMeta.createTimeMs >= checkLabelTimeoutMs) {
            labelMeta.future.completeExceptionally(new RuntimeException("Get label state timeout"));
            LOG.error("Failed to retry to get label state because of timeout, db: {}, table: {}, label: {}, timeout: {}ms",
                    labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label, checkLabelTimeoutMs);
            return;
        }

        scheduledExecutorService.schedule(() -> checkLabelStateByThrift(labelMeta), checkLabelIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("Retry to get label state, db: {}, table: {}, label: {}, retries: {}",
                labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label, labelMeta.numRetries);
    }

    private void checkLabelStateByHttp(LabelMeta labelMeta) {
      labelMeta.numRetries += 1;
      try {
        int hostIndex = ThreadLocalRandom.current().nextInt(hosts.length);
        long startTs = System.currentTimeMillis();
        TransactionStatus status = LabelUtils.getLabelStatus(
            httpClient, hosts[hostIndex], user, password, labelMeta.tableId.db,
            labelMeta.label, objectMapper);
        labelMeta.getLabelCostMs += (System.currentTimeMillis() - startTs);
        if (TransactionStatus.isFinalStatus(status)) {
          labelMeta.transactionStatus = status;
          labelMeta.finishTimeMs = System.currentTimeMillis();
          labelMeta.future.complete(labelMeta);
          long costMs = labelMeta.finishTimeMs - labelMeta.createTimeMs;
          LOG.info(
              "Get final label state, db: {}, table: {}, label: {}, cost: {}ms, retries: {}, status: {}",
              labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label,
              costMs, labelMeta.numRetries, status);
          return;
        }
        LOG.info(
            "Label is not in final status, db: {}, table: {}, label: {}, status: {}",
            labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label,
            status);
        } catch (Exception e) {
            LOG.error("Failed to get label state, db: {}, table: {}, label: {}",
                    labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label, e);
        }
        TableLabelHolder holder = labelHolderMap.get(labelMeta.tableId);
        if (holder == null || holder.getLabel(labelMeta.label) != labelMeta) {
            labelMeta.future.completeExceptionally(new RuntimeException("Label is discarded"));
            LOG.error("Failed to retry to get label state because label is discarded, db: {}, table: {}, label: {}",
                    labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label);
            return;
        }

        if (System.currentTimeMillis() - labelMeta.createTimeMs >= checkLabelTimeoutMs) {
            labelMeta.future.completeExceptionally(new RuntimeException("Get label state timeout"));
            LOG.error("Failed to retry to get label state because of timeout, db: {}, table: {}, label: {}, timeout: {}ms",
                    labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label, checkLabelTimeoutMs);
            return;
        }

        scheduledExecutorService.schedule(() -> checkLabelStateByHttp(labelMeta), checkLabelIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("Retry to get label state, db: {}, table: {}, label: {}, retries: {}",
                labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label, labelMeta.numRetries);
    }

    private void cleanUselessLabels() {
        for (TableLabelHolder labelHolder : labelHolderMap.values()) {
            labelHolder.cleanUselessLabels();
        }
        this.scheduledExecutorService.schedule(this::cleanUselessLabels, 30, TimeUnit.SECONDS);
    }

    private static class TableLabelHolder {
        private final TableId tableId;
        private final ConcurrentHashMap<String, LabelMeta> pendingLabels;

        public TableLabelHolder(TableId tableId) {
            this.tableId = tableId;
            this.pendingLabels = new ConcurrentHashMap<>();
        }

        public LabelMeta addLabel(String label, long expectFinishTimeMs,
                                  long checkLabelIntervalMs) {
          return pendingLabels.computeIfAbsent(
              label,
              key
              -> new LabelMeta(tableId, label, expectFinishTimeMs,
                               checkLabelIntervalMs));
        }

        public LabelMeta getLabel(String label) {
            return pendingLabels.get(label);
        }

        public void cleanUselessLabels() {
            List<String> uselessLabels = new ArrayList<>();
            for (LabelMeta labelMeta : pendingLabels.values()) {
                if (System.currentTimeMillis() - labelMeta.createTimeMs > 5 * 60 * 1000) {
                    uselessLabels.add(labelMeta.label);
                }
            }

            for (String label : uselessLabels) {
                LabelMeta meta = pendingLabels.remove(label);
                if (meta != null) {
                    meta.future.completeExceptionally(new RuntimeException("Clean label because of expiration"));
                    LOG.info("Clean useless label {}", meta.label);
                }
            }
        }
    }

    public static class LabelMeta {
      TableId tableId;
      String label;
      long expectFinishTimeMs;
      long checkLabelIntervalMs;
      CompletableFuture<LabelMeta> future;
      AtomicBoolean isScheduled;
      long createTimeMs;
      long firstDelay = 0;
      int numRetries = 0;
      long finishTimeMs;
      long getClientCostMs = 0;
      long getLabelCostMs = 0;

      TransactionStatus transactionStatus;

      LabelMeta(TableId tableId, String label, long expectFinishTimeMs,
                long checkLabelIntervalMs) {
        this.tableId = tableId;
        this.label = label;
        this.expectFinishTimeMs = expectFinishTimeMs;
        this.checkLabelIntervalMs = checkLabelIntervalMs;
        this.future = new CompletableFuture<>();
        this.isScheduled = new AtomicBoolean(false);
        this.createTimeMs = System.currentTimeMillis();
      }

      public String debugString() {
        long total = finishTimeMs - createTimeMs;
        long wait = firstDelay + (numRetries - 1) * checkLabelIntervalMs;
        return "LabelMeta{" +
            ", label=" + label + ", status=" + transactionStatus +
            ", cost=" + total + ", getClientCost=" + getClientCostMs +
            ", getLabelCost=" + getLabelCostMs + ", wait=" + wait +
            ", pending=" + (total - wait - getLabelCostMs) +
            ", retry=" + (numRetries - 1) + '}';
      }
    }

    public static class LabelMetaConfig {
        StreamLoadProperties properties;
    }
}
