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
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LabelManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(LabelManager.class);

    private final String[] hosts;
    private final String user;
    private final String password;
    private final long retryIntervalMs;
    private final Map<TableId, TableLabelHolder> labelHolderMap;
    private transient ScheduledExecutorService scheduledExecutorService;
    private transient ObjectMapper objectMapper;

    public LabelManager(StreamLoadProperties properties) {
        this.hosts = properties.getLoadUrls();
        this.user = properties.getUsername();
        this.password = properties.getPassword();
        this.retryIntervalMs = properties.getCheckLabelIntervalMs();
        this.labelHolderMap = new ConcurrentHashMap<>();
    }

    public void start() {
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(
                Integer.MAX_VALUE,
                r -> {
                    Thread thread = new Thread(null, r, "LabelManager-" + UUID.randomUUID());
                    thread.setDaemon(true);
                    return thread;
                });

        this.objectMapper = new ObjectMapper();
        // StreamLoadResponseBody does not contain all fields returned by StarRocks
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // filed names in StreamLoadResponseBody are case-insensitive
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
    }

    @Override
    public void close() {
        if (scheduledExecutorService != null) {
            this.scheduledExecutorService.shutdownNow();
        }
        LOG.info("Stop label manager");
    }

    public CompletableFuture<TransactionStatus> getLabelFinalStatusAsync(
            TableId tableId, String label, long expectFinishTimeMs) {
        LabelMeta labelMeta = labelHolderMap.computeIfAbsent(tableId, key -> new TableLabelHolder(tableId))
                .addLabel(label, expectFinishTimeMs);
        if (labelMeta.isScheduled.compareAndSet(false, true)) {
            long delayMs = expectFinishTimeMs > 0 ? Math.max(0, System.currentTimeMillis() - expectFinishTimeMs) : retryIntervalMs;
            scheduledExecutorService.schedule(() -> checkLabelState(labelMeta), delayMs, TimeUnit.MILLISECONDS);
        }
        return labelMeta.future;
    }

    public void clear() {
        labelHolderMap.values().forEach(TableLabelHolder::clear);
    }

    private void checkLabelState(LabelMeta labelMeta) {
        try {
            int hostIndex = ThreadLocalRandom.current().nextInt(hosts.length);
            TransactionStatus status = LabelUtils.getLabelStaus(
                    hosts[hostIndex], user, password, labelMeta.tableId.db, labelMeta.label, objectMapper);
            if (TransactionStatus.isFinalStatus(status)) {
                labelMeta.future.complete(status);
                return;
            }
            LOG.info("Label is not in final status, db: {}, table: {}, label: {}, status: {}",
                    labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label, status);
        } catch (Exception e) {
            LOG.error("Failed to get label state, db: {}, table: {}, label: {}",
                    labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label, e);
        }
        TableLabelHolder holder = labelHolderMap.get(labelMeta.tableId);
        if (holder != null && holder.getLabel(labelMeta.label) == labelMeta) {
            scheduledExecutorService.schedule(() -> checkLabelState(labelMeta), retryIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    private static class TableLabelHolder {
        private final TableId tableId;
        private final Map<String, LabelMeta> pendingLabels;

        public TableLabelHolder(TableId tableId) {
            this.tableId = tableId;
            this.pendingLabels = new ConcurrentHashMap<>();
        }

        public LabelMeta addLabel(String label, long expectFinishTimeMs) {
            return pendingLabels.computeIfAbsent(label, key -> new LabelMeta(tableId, label, expectFinishTimeMs));
        }

        public LabelMeta getLabel(String label) {
            return pendingLabels.get(label);
        }

        public void clear() {
            pendingLabels.clear();
        }
    }

    private static class LabelMeta {
        TableId tableId;
        String label;
        long expectFinishTimeMs;
        CompletableFuture<TransactionStatus> future;
        AtomicBoolean isScheduled;

        LabelMeta(TableId tableId, String label, long expectFinishTimeMs) {
            this.tableId = tableId;
            this.label = label;
            this.expectFinishTimeMs = expectFinishTimeMs;
            this.future = new CompletableFuture<>();
            this.isScheduled = new AtomicBoolean(false);
        }
    }
}
