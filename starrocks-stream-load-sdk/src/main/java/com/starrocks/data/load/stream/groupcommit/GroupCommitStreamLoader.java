
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

import com.starrocks.data.load.stream.DefaultStreamLoader;
import com.starrocks.data.load.stream.StreamLoadConstants;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.TransactionStatus;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class GroupCommitStreamLoader extends DefaultStreamLoader {

    private static final Logger LOG = LoggerFactory.getLogger(GroupCommitStreamLoader.class);

    private final LabelManager labelManager;

    public GroupCommitStreamLoader(LabelManager labelManager) {
        this.labelManager = labelManager;
    }

    public ScheduledFuture<?> scheduleFlush(GroupCommitTable table, long chunkId, int delayMs) {
        return executorService.schedule(() -> table.checkFlushInterval(chunkId), delayMs, TimeUnit.MILLISECONDS);
    }

    public void sendLoad(LoadRequest request, int delayMs) {
        executorService.schedule(() -> send(request), delayMs, TimeUnit.MILLISECONDS);
    }

    private void send(LoadRequest request) {
        GroupCommitTable groupCommitTable = request.getTable();
        String database = groupCommitTable.getDatabase();
        String table = groupCommitTable.getTable();
        try {
            String host = getAvailableHost();
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
        CompletableFuture<TransactionStatus> future = labelManager.getLabelFinalStatusAsync(
                    TableId.of(table.getDatabase(), table.getTable()),
                    request.getResponse().getBody().getLabel(), expectFinishTimeMs)
                .whenCompleteAsync(
                        (status, throwable) -> dealLabelStatus(request, status, throwable),
                        executorService);
        request.setFuture(future);
    }

    private void dealLabelStatus(LoadRequest request, TransactionStatus status, Throwable throwable) {
        if (throwable != null) {
            request.getTable().loadFinish(request, throwable);
            return;
        }

        if (status != TransactionStatus.VISIBLE && status != TransactionStatus.COMMITTED) {
            request.getTable().loadFinish(request, new RuntimeException(
                    String.format("Label %s does not in final status, current status: %s",
                            request.getResponse().getBody().getLabel(), status)));
        } else {
            request.getTable().loadFinish(request, null);
        }
    }
}
