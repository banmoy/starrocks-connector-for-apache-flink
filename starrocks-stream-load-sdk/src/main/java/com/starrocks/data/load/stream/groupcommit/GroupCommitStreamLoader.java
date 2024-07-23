
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

import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.RpcCallback;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.channel.ChannelType;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;
import com.starrocks.data.load.stream.DefaultStreamLoader;
import com.starrocks.data.load.stream.StreamLoadConstants;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.TransactionStatus;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupCommitStreamLoader extends DefaultStreamLoader {

    private static final Logger LOG = LoggerFactory.getLogger(GroupCommitStreamLoader.class);

    public GroupCommitStreamLoader() {
    }

    @Override
    public void start(StreamLoadProperties properties, StreamLoadManager manager) {
        RpcClientOptions clientOptions = new RpcClientOptions();
        clientOptions.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        clientOptions.setConnectTimeoutMillis(1000);
        clientOptions.setReadTimeoutMillis(1000);
        clientOptions.setWriteTimeoutMillis(1000);
        clientOptions.setChannelType(ChannelType.POOLED_CONNECTION);
        clientOptions.setMaxTotalConnections(10);
        clientOptions.setMinIdleConnections(2);
        clientOptions.setMaxTryTimes(3);
        clientOptions.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
        clientOptions.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
        clientOptions.setIoThreadNum(Runtime.getRuntime().availableProcessors());
        clientOptions.setWorkThreadNum(Runtime.getRuntime().availableProcessors());
        BrpcClientManager.BrpcConfig brpcConfig = new BrpcClientManager.BrpcConfig(clientOptions);
        BrpcClientManager.getInstance().takeRef(brpcConfig);

        FeMetaService.FeMetaConfig config = new FeMetaService.FeMetaConfig();
        config.properties = properties;
        config.numExecutors = 2;
        config.updateIntervalMs = 60000;
        FeMetaService.getInstance().takeRef(config);

        LabelMetaService.LabelMetaConfig labelMetaConfig = new LabelMetaService.LabelMetaConfig();
        labelMetaConfig.properties = properties;
        LabelMetaService.getInstance().takeRef(labelMetaConfig);

        super.start(properties, manager);
    }

    @Override
    public void close() {
        super.close();
        LabelMetaService.getInstance().releaseRef();
        FeMetaService.getInstance().releaseRef();
        BrpcClientManager.getInstance().releaseRef();
    }

    public ScheduledFuture<?> scheduleFlush(GroupCommitTable table, long chunkId, int delayMs) {
        return executorService.schedule(() -> table.checkFlushInterval(chunkId), delayMs, TimeUnit.MILLISECONDS);
    }

    public void sendLoad(LoadRequest request, int delayMs) {
        executorService.schedule(() -> sendBrpc(request), delayMs, TimeUnit.MILLISECONDS);
    }

    // TODO check FE availability without connection each time
    @Override
    protected String getAvailableHost() {
        String[] hosts = properties.getLoadUrls();
        return hosts.length == 0 ? null : hosts[ThreadLocalRandom.current().nextInt(hosts.length)];
    }

    private String getAvailableBeHttpHost(String db, String table) throws Exception {
        CompletableFuture<FeMetaService.BeMetas> future = FeMetaService.getInstance()
                .getTableBeMetas(TableId.of(db, table));
        List<WorkerAddress> addresses = future.get().getHttpAddresses();
        return (addresses == null || addresses.isEmpty()) ? null
                : "http://" + addresses.get(ThreadLocalRandom.current().nextInt(addresses.size())).toString();
    }

    private WorkerAddress getAvailableBeBrpcHost(String db, String table) throws Exception {
        CompletableFuture<FeMetaService.BeMetas> future = FeMetaService.getInstance()
                .getTableBeMetas(TableId.of(db, table));
        List<WorkerAddress> addresses = future.get().getBrpcAddresses();
        return (addresses == null || addresses.isEmpty()) ? null
                : addresses.get(ThreadLocalRandom.current().nextInt(addresses.size()));
    }

    private void sendBrpc(LoadRequest loadRequest) {
      loadRequest.executeTimeMs = System.currentTimeMillis();
      GroupCommitTable groupCommitTable = loadRequest.getTable();
      String database = groupCommitTable.getDatabase();
      String table = groupCommitTable.getTable();
      try {
        HttpEntity entity =
            groupCommitTable.getHttpEntity(loadRequest.getChunk());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        entity.writeTo(outputStream);
        byte[] data = outputStream.toByteArray();
        loadRequest.rawSize = loadRequest.getChunk().estimateChunkSize();
        loadRequest.compressSize = data.length;
        loadRequest.compressTimeMs = System.currentTimeMillis();

        WorkerAddress brpcAddress = getAvailableBeBrpcHost(database, table);
        PBackendServiceAsync service =
            BrpcClientManager.getInstance().getBackendService(brpcAddress);
        String userLabel = UUID.randomUUID().toString();
        loadRequest.setLabel(userLabel);

        PGroupCommitLoadRequest request = new PGroupCommitLoadRequest();
        request.setDb(database);
        request.setTable(table);
        request.setUserLabel(userLabel);
        request.setTimeout(timeout);
        request.setClientTimeMs(System.currentTimeMillis());
        RpcContext.getContext().setRequestBinaryAttachment(data);
        LoadRpcCallback callback = new LoadRpcCallback(loadRequest);
        service.groupCommitLoad(request, callback);
        loadRequest.callRpcTimeMs = System.currentTimeMillis();

        LOG.info(
            "Send group commit load request, db: {}, table: {}, user label: {}, chunkId: {}",
            database, table, request.getUserLabel(),
            loadRequest.getChunk().getChunkId());
        } catch (Exception e) {
            LOG.error("Failed to send load brpc, db: {}, table: {}, chunkId: {}",
                    database, table, loadRequest.getChunk().getChunkId(), e);
            groupCommitTable.loadFinish(loadRequest, e);
        }
    }

    private void sendHttp(LoadRequest request) {
        GroupCommitTable groupCommitTable = request.getTable();
        String database = groupCommitTable.getDatabase();
        String table = groupCommitTable.getTable();
        try {
            String host = getAvailableBeHttpHost(database, table);
            String sendUrl = getSendUrl(host, database, table);

            HttpPut httpPut = new HttpPut(sendUrl);
            httpPut.setConfig(RequestConfig.custom()
                    .setSocketTimeout(properties.getSocketTimeout())
                    .setExpectContinueEnabled(true)
                    .setRedirectsEnabled(true)
                    .build());
            httpPut.setEntity(groupCommitTable.getHttpEntity(request.getChunk()));
            httpPut.setHeaders(defaultHeaders);
            StreamLoadTableProperties tableProperties = groupCommitTable.getProperties();
            for (Map.Entry<String, String> entry : tableProperties.getProperties().entrySet()) {
                httpPut.removeHeaders(entry.getKey());
                httpPut.addHeader(entry.getKey(), entry.getValue());
            }

            String label = UUID.randomUUID().toString();
            request.setLabel(label);
            httpPut.addHeader("label", label);
            httpPut.addHeader("client_time_ms", String.valueOf(System.currentTimeMillis()));

            LOG.info("Send group commit load request, db: {}, table: {}, user label: {}, chunkId: {}",
                    database, table, label, request.getChunk().getChunkId());
            try (CloseableHttpClient client = clientBuilder.build()) {
                long startNanoTime = System.nanoTime();
                String responseBody;
                try (CloseableHttpResponse response = client.execute(httpPut)) {
                    responseBody = parseHttpResponse("load", database, table, label, response);
                }

                LOG.info("Receive group commit load response, db: {}, table: {}, user label: {}, chunkId: {}, " +
                                "response: {}", database, table, label, request.getChunk().getChunkId(), responseBody);

                StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
                StreamLoadResponse.StreamLoadResponseBody streamLoadBody =
                        objectMapper.readValue(responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
                streamLoadResponse.setBody(streamLoadBody);
                String status = streamLoadBody.getStatus();
                if (status == null) {
                    throw new StreamLoadFailException(String.format("Stream load status is null. db: %s, table: %s, " +
                            "user label: %s, response body: %s", database, table, label, responseBody));
                }

                if (StreamLoadConstants.RESULT_STATUS_SUCCESS.equals(status)) {
                    streamLoadResponse.setCostNanoTime(System.nanoTime() - startNanoTime);
                    request.setResponse(streamLoadResponse);
                    waitLabelAsync(request);
                } else {
                    String errorMsg = String.format("Stream load failed because of error, db: %s, table: %s, user label: %s, " +
                                    "\nresponseBody: %s", database, table, label, responseBody);
                    throw new StreamLoadFailException(errorMsg, streamLoadBody);
                }
            } catch (StreamLoadFailException e) {
                throw e;
            }  catch (Exception e) {
                String errorMsg = String.format("Stream load failed because of unknown exception, db: %s, table: %s, " +
                        "user label: %s", database, table, label);
                throw new StreamLoadFailException(errorMsg, e);
            }
        } catch (Exception e) {
            groupCommitTable.loadFinish(request, e);
        }
    }

    private void waitLabelAsync(LoadRequest request) {
        GroupCommitTable table = request.getTable();
        long leftTimeMs = request.getResponse().getBody().getLeftTimeMs();
        long expectFinishTimeMs = leftTimeMs > 0 ? System.currentTimeMillis() + leftTimeMs : -1;
        CompletableFuture<LabelMetaService.LabelMeta> future =
            LabelMetaService.getInstance()
                .getLabelFinalStatusAsync(
                    TableId.of(table.getDatabase(), table.getTable()),
                    request.getResponse().getBody().getLabel(),
                    expectFinishTimeMs)
                .whenCompleteAsync(
                    (labelMeta, throwable)
                        -> dealLabelStatus(request, labelMeta, throwable),
                    executorService);
        request.setFuture(future);
    }

    private void dealLabelStatus(LoadRequest request,
                                 LabelMetaService.LabelMeta labelMeta,
                                 Throwable throwable) {
      TransactionStatus status = labelMeta.transactionStatus;
      request.labelFinalTimeMs = System.currentTimeMillis();
      if (throwable != null) {
        request.getTable().loadFinish(request, throwable);
        return;
      }

      if (status != TransactionStatus.VISIBLE &&
          status != TransactionStatus.COMMITTED) {
        request.getTable().loadFinish(
            request,
            new RuntimeException(String.format(
                "Label %s does not in final status, current status: %s",
                request.getResponse().getBody().getLabel(), status)));
      } else {
        request.getTable().loadFinish(request, null);
        logRequestTrace(request, labelMeta);
      }
    }

    private class LoadRpcCallback implements RpcCallback<PGroupCommitLoadResponse> {

      private final LoadRequest request;

      public LoadRpcCallback(LoadRequest request) { this.request = request; }

      @Override
      public void success(PGroupCommitLoadResponse response) {
        request.receiveResponseTimeMs = System.currentTimeMillis();
        String db = request.getTable().getDatabase();
        String table = request.getTable().getTable();

        LOG.info(
            "Receive group commit load response, db: {}, table: {}, user label: {}, chunkId: {}, ",
            db, table, request.getLabel(), request.getChunk().getChunkId());

        request.loadResponse = response;
        StreamLoadResponse.StreamLoadResponseBody streamLoadBody =
            new StreamLoadResponse.StreamLoadResponseBody();
        streamLoadBody.setTxnId(response.getTxnId());
        streamLoadBody.setLabel(response.getLabel());
        streamLoadBody.setStatus(response.getStatus());
        streamLoadBody.setMessage(response.getMessage());
        streamLoadBody.setLeftTimeMs(response.getLeftTimeMs());

        StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
        streamLoadResponse.setBody(streamLoadBody);
        String status = streamLoadBody.getStatus();
        if (StreamLoadConstants.RESULT_STATUS_SUCCESS.equals(status)) {
          request.setResponse(streamLoadResponse);
          waitLabelAsync(request);
        } else {
          String errorMsg = String.format(
              "Stream load failed because of error, db: %s, table: %s, user label: %s, "
                  + "response: %s",
              db, table, request.getLabel(), response);
          Throwable throwable =
              new StreamLoadFailException(errorMsg, streamLoadBody);
          request.getTable().loadFinish(request, throwable);
        }
        }

        @Override
        public void fail(Throwable throwable) {
            String db = request.getTable().getDatabase();
            String table = request.getTable().getTable();
            LOG.error("Send group commit load failure, db: {}, table: {}, user label: {}, chunkId: {}",
                    db, table, request.getLabel(), request.getChunk().getChunkId(), throwable);
            String errorMsg = String.format("Send group commit load failure, db: %s, table: %s, user label: %s",
                    db, table, request.getLabel());
            Throwable exception = new StreamLoadFailException(errorMsg, throwable);
            request.getTable().loadFinish(request, exception);
        }
    }

    private static void logRequestTrace(LoadRequest request,
                                        LabelMetaService.LabelMeta labelMeta) {
      LOG.info(
          "Cost trace, db: {}, table: {}, chunkId: {}, userLabel: {}, raw/compress: {}/{}, "
              +
              "total: {}, pending: {}, compress: {}, callRpc: {}, transmit: {}, server: {}, response: {}, "
              +
              "waitLabel: {}, copyData: {}, group: {}, pending: {}, waitPlan: {}, append: {}, requestPlanNum: {}, "
              + "{}",
          request.getTable().getDatabase(), request.getTable().getTable(),
          request.getChunk().getChunkId(), request.getLabel(), request.rawSize,
          request.compressSize, request.labelFinalTimeMs - request.createTimeMs,
          request.executeTimeMs - request.createTimeMs,
          request.compressTimeMs - request.executeTimeMs,
          request.callRpcTimeMs - request.compressTimeMs,
          request.loadResponse.getNetworkCostMs(),
          request.loadResponse.getLoadCostMs(),
          request.receiveResponseTimeMs - request.loadResponse.getFinishTs(),
          request.labelFinalTimeMs - request.receiveResponseTimeMs,
          request.loadResponse.getCopyDataMs(),
          request.loadResponse.getGroupCommitMs(),
          request.loadResponse.getPendingMs(),
          request.loadResponse.getWaitPlanMs(),
          request.loadResponse.getAppendMs(),
          request.loadResponse.getRequestPlanNum(), labelMeta.debugString());
    }
}
