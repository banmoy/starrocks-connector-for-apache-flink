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
import com.starrocks.data.load.stream.StreamLoadUtils;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class FeMetaService extends SharedService<FeMetaService.FeMetaConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(FeMetaService.class);

    private static final FeMetaService INSTANCE = new FeMetaService();

    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<TableId, TableBeInfo> beInfoMap;
    private volatile StreamLoadProperties properties;
    private volatile Header[] defaultHeaders;
    private HttpClientBuilder clientBuilder;
    private volatile int updateIntervalMs;
    private ScheduledExecutorService executorService;

    private FeMetaService() {
        this.objectMapper = new ObjectMapper();
        // StreamLoadResponseBody does not contain all fields returned by StarRocks
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // filed names in StreamLoadResponseBody are case-insensitive
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

        this.beInfoMap = new ConcurrentHashMap<>();
    }

    @Override
    protected void init(FeMetaConfig config) {
        this.executorService = Executors.newScheduledThreadPool(
                config.numExecutors,
                r -> {
                    Thread thread = new Thread(null, r, "FeMetaService-" + UUID.randomUUID());
                    thread.setDaemon(true);
                    return thread;
                }
        );
        this.properties = config.properties;
        this.updateIntervalMs = config.updateIntervalMs;
        initDefaultHeaders(properties);
        this.clientBuilder  = HttpClients.custom()
                .setRequestExecutor(new HttpRequestExecutor(properties.getWaitForContinueTimeoutMs()))
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });
        LOG.info("Init fe meta service, numExecutors: {}, updateIntervalMs: {}", config.numExecutors, updateIntervalMs);
    }

    protected void initDefaultHeaders(StreamLoadProperties properties) {
        Map<String, String> headers = new HashMap<>(properties.getHeaders());
        headers.put(HttpHeaders.AUTHORIZATION, StreamLoadUtils.getBasicAuthHeader(properties.getUsername(), properties.getPassword()));
        headers.put(HttpHeaders.EXPECT, "100-continue");
        this.defaultHeaders = headers.entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);
    }

    @Override
    protected void reset() {
        if (this.executorService != null) {
            this.executorService.shutdownNow();
            this.executorService = null;
        }
        beInfoMap.clear();
        LOG.info("Reset fe meta service");
    }

    public CompletableFuture<List<WorkerAddress>> getTableBeAddress(TableId tableId) {
        return beInfoMap.computeIfAbsent(tableId, this::createTableInfo).futureRef.get();
    }

    private TableBeInfo createTableInfo(TableId tableId) {
        TableBeInfo tableBeInfo = new TableBeInfo(tableId);
        this.executorService.schedule(() -> getTableBeInfo(tableBeInfo), 0, TimeUnit.MILLISECONDS);
        return tableBeInfo;
    }

    private void getTableBeInfo(TableBeInfo info) {
        String database = info.tableId.db;
        String table = info.tableId.table;
        try {
            String host = getAvailableHost();
            String sendUrl = getSendUrl(host, database, table);

            HttpPut httpPut = new HttpPut(sendUrl);
            httpPut.setConfig(RequestConfig.custom()
                    .setSocketTimeout(properties.getSocketTimeout())
                    .setExpectContinueEnabled(true)
                    .setRedirectsEnabled(true)
                    .build());
            httpPut.setHeaders(defaultHeaders);
            httpPut.addHeader("meta", "true");
            try (CloseableHttpClient client = clientBuilder.build()) {
                String responseBody;
                try (CloseableHttpResponse response = client.execute(httpPut)) {
                    responseBody = parseHttpResponse(database, table, response);
                }

                Response response = objectMapper.readValue(responseBody, Response.class);
                String status = response.getStatus();
                if (status == null) {
                    throw new StreamLoadFailException(String.format("Stream load status is null. db: %s, table: %s, " +
                            "response body: %s", database, table, responseBody));
                }

                String beAddressList = response.getBeAddressList();
                if (beAddressList != null && !beAddressList.isEmpty()) {
                    List<WorkerAddress> addresses = parseAddresses(beAddressList);
                    info.updateAddresses(addresses);
                } else {
                    throw new StreamLoadFailException(
                            String.format("empty be addresses, db: %s, table: %s", database, table));
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to get meta, db: {}, table: {}", database, table, e);
        }
        this.executorService.schedule(() -> getTableBeInfo(info), updateIntervalMs, TimeUnit.MILLISECONDS);
    }

    private List<WorkerAddress> parseAddresses(String beAddressList) {
        List<WorkerAddress> workerAddresses = new ArrayList<>();
        String[] bes = beAddressList.split(";");
        for (String be : bes) {
            String[] parts = be.split(":");
            workerAddresses.add(new WorkerAddress(parts[0], parts[1]));
        }
        return workerAddresses;
    }


    private String parseHttpResponse(String db, String table, CloseableHttpResponse response) throws StreamLoadFailException {
        int code = response.getStatusLine().getStatusCode();
        if (401 == code) {
            String errorMsg = String.format("Access denied. You need to grant at least SELECT and INSERT " +
                    "privilege on %s.%s. response status line: %s", db, table, response.getStatusLine());
            LOG.error("{}", errorMsg);
            throw new StreamLoadFailException(errorMsg);
        } else if (200 != code) {
            String errorMsg = String.format("Request failed because http response code is not 200. db: %s, table: %s," +
                    " response status line: %s", db, table, response.getStatusLine());
            LOG.error("{}", errorMsg);
            throw new StreamLoadFailException(errorMsg);
        }

        HttpEntity respEntity = response.getEntity();
        if (respEntity == null) {
            String errorMsg = String.format("Request failed because response entity is null. db: %s, table: %s," +
                    "response status line: %s", db, table, response.getStatusLine());
            LOG.error("{}", errorMsg);
            throw new StreamLoadFailException(errorMsg);
        }

        try {
            return EntityUtils.toString(respEntity);
        } catch (Exception e) {
            String errorMsg = String.format("Request failed because fail to convert response entity to string. " +
                            "db: %s, table: %s, response status line: %s, response entity: %s", db,
                    table, response.getStatusLine(), response.getEntity());
            LOG.error("{}", errorMsg, e);
            throw new StreamLoadFailException(errorMsg, e);
        }
    }

    protected String getSendUrl(String host, String database, String table) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        return host + "/api/" + database + "/" + table + "/_stream_load";
    }

    protected String getAvailableHost() {
        String[] hosts = properties.getLoadUrls();
        return hosts.length == 0 ? null : hosts[ThreadLocalRandom.current().nextInt(hosts.length)];
    }

    public static FeMetaService getInstance() {
        return INSTANCE;
    }

    private static class TableBeInfo {
        TableId tableId;
        AtomicReference<CompletableFuture<List<WorkerAddress>>> futureRef;

        public TableBeInfo(TableId tableId) {
            this.tableId = tableId;
            this.futureRef = new AtomicReference<>(new CompletableFuture<>());
        }

        public void updateAddresses(List<WorkerAddress> addresses) {
            CompletableFuture<List<WorkerAddress>> oldFuture = futureRef.get();
            if (oldFuture.complete(addresses)) {
                return;
            }

            CompletableFuture<List<WorkerAddress>> newFuture = new CompletableFuture<>();
            newFuture.complete(addresses);
            futureRef.compareAndSet(oldFuture, newFuture);
        }
    }

    public static class FeMetaConfig {
        StreamLoadProperties properties;
        int numExecutors = 2;
        int updateIntervalMs = 60000;
    }

    public static class Response {
        private String status;
        String beAddressList;

        public String getStatus() {
            return status;
        }

        public String getBeAddressList() {
            return beAddressList;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public void setBeAddressList(String beAddressList) {
            this.beAddressList = beAddressList;
        }
    }
}
