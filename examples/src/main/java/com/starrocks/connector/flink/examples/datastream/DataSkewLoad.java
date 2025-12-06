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

package com.starrocks.connector.flink.examples.datastream;

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This example will show how to load records to StarRocks table using Flink DataStream.
 * Each record is a json {@link String} in Flink, and will be loaded as a row of StarRocks table.
 */
public class DataSkewLoad {

    public static void main(String[] args) throws Exception {
        // testGenData();
        runJob(args);
    }


    private static void runJob(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        String jdbcUrl = params.get("jdbcUrl", "jdbc:mysql://127.0.0.1:9030");
        String loadUrl = params.get("loadUrl", "127.0.0.1:8030");
        String format = params.get("format", "csv");
        String dbName = params.get("db", "prod_postgres_cdc");
        String tableName = params.get("table", "Customers");
        String companyIdParam = params.get("companyIds", "");
        String[] companyIds = COMPANY_IDS;
        if (!companyIdParam.isEmpty()) {
            companyIds = companyIdParam.split(",");
        }
        int rate = params.getInt("rate", 10000);
        int batch = params.getInt("batch", 10);
        FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
        rateLimiter.setRate(rate);
        int sourceParallel = params.getInt("srcParallel", 2);
        int sinkParallel = params.getInt("sinkParallel", 2);
        String flushIntervalMs = params.get("flushIntervalMs", "1000");
        int strLen = params.getInt("strLen", 7);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().disableCheckpointing();

        DataStream<String> source = env.addSource(new DataSource(companyIds, format, strLen, rateLimiter, batch))
                .setParallelism(sourceParallel).disableChaining();
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", jdbcUrl)
                .withProperty("load-url", loadUrl)
                .withProperty("database-name", dbName)
                .withProperty("table-name", tableName)
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("sink.buffer-flush.interval-ms", flushIntervalMs)
                .withProperty("sink.buffer-flush.max-bytes", "188743680")
                .withProperty("sink.properties.format", format)
                .withProperty("sink.properties.column_separator", CSV_DELIMITER)
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();
        SinkFunction<String> starRockSink = StarRocksSink.sink(options);
        source.addSink(starRockSink).setParallelism(sinkParallel);
        env.execute("DataSkewLoad");
    }

    public static class DataSource extends RichParallelSourceFunction<String> {

        private final String[] companyIds;
        private final String format;
        private final int strLen;
        private final FlinkConnectorRateLimiter rateLimiter;
        private final int batchSize;
        private transient boolean cancel;

        public DataSource(String[] companyIds, String format, int strLen, FlinkConnectorRateLimiter rateLimiter, int batchSize) {
            this.companyIds = companyIds;
            this.format = format;
            this.strLen = strLen;
            this.rateLimiter = rateLimiter;
            this.batchSize = batchSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            rateLimiter.open(getRuntimeContext());
            this.cancel = false;
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            while (!cancel) {
                rateLimiter.acquire(batchSize);
                synchronized (sourceContext.getCheckpointLock()) {
                    for (int i = 0; i < batchSize; i++) {
                        int index = random.nextInt(companyIds.length);
                        String row = genData(companyIds[index], format, strLen);
                        sourceContext.collect(row);
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void cancel() {
            this.cancel = true;
        }
    }

    private static final String[] COMPANY_IDS = {"14", "10", "100", "13", "16", "12", "15", "11"};
    private static final Random random = new Random();
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String BASE_DATE = "2025-12-06";

    // Character set for random string generation
    private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final String CSV_DELIMITER = "|";

    /**
     * Generate customer data
     * @param companyId If null or empty, randomly select a companyId; otherwise use the specified one
     * @return JSON string representing column name to value mapping
     */
    public static String genData(String companyId, String format, int strLen) {
        // Select companyId
        String selectedCompanyId;
        if (companyId == null || companyId.isEmpty()) {
            selectedCompanyId = COMPANY_IDS[random.nextInt(COMPANY_IDS.length)];
        } else {
            selectedCompanyId = companyId;
        }

        // Generate id based on companyId
        int id;
        if ("10".equals(selectedCompanyId)) {
            id = random.nextInt(1200000) + 1; // 1 to 1200000
        } else {
            id = random.nextInt(350000) + 1; // 1 to 350000
        }

        // Generate createdAt - fixed for each <id, companyId> pair
        String createdAt = generateCreatedAt(id, selectedCompanyId);

        // Generate updatedAt - current time
        String updatedAt = LocalDateTime.now().format(DATE_TIME_FORMATTER);

        // Generate random strings for varchar and json fields
        String name = generateRandomString(strLen);
        String email = generateRandomString(strLen);
        String source = generateRandomString(strLen);
        String integrationAccountId = generateRandomString(strLen);
        String shippingAddress = generateRandomString(strLen);
        String billingAddress = generateRandomString(strLen);
        String affiliatedEntityId = generateRandomString(strLen);
        String externalSourceId = generateRandomString(strLen);

        if (format.equalsIgnoreCase("csv")) {
            // Build CSV in DDL column order: id, companyId, createdAt, name, email, source,
            // integrationAccountId, shippingAddress, billingAddress, updatedAt, affiliatedEntityId,
            // externalSourceId, deletedAt
            StringBuilder builder = new StringBuilder();
            builder.append(id).append(CSV_DELIMITER);
            builder.append(selectedCompanyId).append(CSV_DELIMITER);
            builder.append(createdAt).append(CSV_DELIMITER);
            builder.append(name).append(CSV_DELIMITER);
            builder.append(email).append(CSV_DELIMITER);
            builder.append(source).append(CSV_DELIMITER);
            builder.append(integrationAccountId).append(CSV_DELIMITER);
            builder.append(shippingAddress).append(CSV_DELIMITER);
            builder.append(billingAddress).append(CSV_DELIMITER);
            builder.append(updatedAt).append(CSV_DELIMITER);
            builder.append(affiliatedEntityId).append(CSV_DELIMITER);
            builder.append(externalSourceId).append(CSV_DELIMITER);
            builder.append("\\N");

            return builder.toString();
        }

        // Build JSON
        Map<String, Object> data = new HashMap<>();
        data.put("id", String.valueOf(id));
        data.put("companyId", selectedCompanyId);
        data.put("createdAt", createdAt);
        data.put("name", name);
        data.put("email", email);
        data.put("source", source);
        data.put("integrationAccountId", integrationAccountId);
        data.put("shippingAddress", shippingAddress);
        data.put("billingAddress", billingAddress);
        data.put("updatedAt", updatedAt);
        data.put("affiliatedEntityId", affiliatedEntityId);
        data.put("externalSourceId", externalSourceId);
        data.put("deletedAt", null);

        return mapToJson(data);
    }

    /**
     * Generate createdAt that is fixed for each <id, companyId> pair
     * Uses hash function to ensure determinism
     */
    private static String generateCreatedAt(int id, String companyId) {
        // Use hash to generate deterministic time within the day
        int hash = (id + companyId.hashCode()) & 0x7FFFFFFF; // Ensure positive
        int secondsInDay = 24 * 60 * 60;
        int seconds = hash % secondsInDay;

        int hours = seconds / 3600;
        int minutes = (seconds % 3600) / 60;
        int secs = seconds % 60;

        return String.format("%s %02d:%02d:%02d", BASE_DATE, hours, minutes, secs);
    }

    /**
     * Generate random string of specified length
     */
    private static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARS.charAt(random.nextInt(CHARS.length())));
        }
        return sb.toString();
    }

    /**
     * Convert Map to JSON string
     */
    private static String mapToJson(Map<String, Object> map) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) {
                json.append(",");
            }
            first = false;
            json.append("\"").append(entry.getKey()).append("\":");
            Object value = entry.getValue();
            if (value == null) {
                json.append("null");
            } else {
                json.append(value);
            }
        }
        json.append("}");
        return json.toString();
    }

    private static void testGenData() {
        // Test with random companyId
        System.out.println("Random companyId:");
        System.out.println(genData(null, "csv", 7));
        System.out.println();

        // Test with specific companyId "10"
        System.out.println("CompanyId = 10:");
        System.out.println(genData("10", "csv", 7));
        System.out.println();

        // Test with specific companyId "14"
        System.out.println("CompanyId = 14:");
        System.out.println(genData("14", "csv", 7));
    }
}