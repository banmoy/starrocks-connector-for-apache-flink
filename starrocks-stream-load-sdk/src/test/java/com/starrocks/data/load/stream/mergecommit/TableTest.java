/*
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

package com.starrocks.data.load.stream.mergecommit;

import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableTest {

    /**
     * Mock MergeCommitLoader for testing
     */
    private static class MockMergeCommitLoader extends MergeCommitLoader {
        @Override
        public ScheduledFuture<?> scheduleFlush(Table table, long chunkId, int delayMs) {
            return null;
        }

        @Override
        public void sendLoad(LoadRequest.RequestRun requestRun, int delayMs) {
            // no-op
        }

        @Override
        public void start(StreamLoadProperties properties, StreamLoadManager manager) {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /**
     * Mock MergeCommitManager for testing
     */
    private static class MockMergeCommitManager extends MergeCommitManager {
        public MockMergeCommitManager() {
            super(createMinimalProperties());
        }

        private static StreamLoadProperties createMinimalProperties() {
            return StreamLoadProperties.builder()
                    .loadUrls("http://localhost:8030")
                    .username("root")
                    .password("")
                    .defaultTableProperties(StreamLoadTableProperties.builder()
                            .database("db").table("tbl").build())
                    .build();
        }

        @Override
        public void onLoadStart(Table table, long dataSize, int numRows) {
            // no-op
        }

        @Override
        public void onLoadSuccess(Table table, LoadRequest request) {
            // no-op
        }

        @Override
        public void onLoadFailure(Table table, LoadRequest request, Throwable throwable) {
            // no-op
        }

        @Override
        public void releaseCache(LoadRequest request) {
            // no-op
        }
    }

    private Map<String, String> createMap(String... keyValues) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    private Table createTable(StreamLoadTableProperties properties) {
        return new Table(
                properties.getDatabase(),
                properties.getTable(),
                new MockMergeCommitManager(),
                new MockMergeCommitLoader(),
                properties,
                3,      // maxRetries
                1000,   // retryIntervalInMs
                5000,   // flushIntervalMs
                1024 * 1024, // chunkSize
                10      // maxInflightRequests
        );
    }

    @Test
    public void testGetLoadTimeoutMsWithDefaultTimeout() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        Table table = createTable(properties);

        // Default timeout is 600 seconds = 600000 ms
        assertEquals(LoadParameters.DEFAULT_TIMEOUT_SECONDS * 1000, table.getLoadTimeoutMs());
    }

    @Test
    public void testGetLoadTimeoutMsWithCustomTimeout() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addProperty("timeout", "300")
                .build();

        Table table = createTable(properties);

        // Custom timeout is 300 seconds = 300000 ms
        assertEquals(300 * 1000, table.getLoadTimeoutMs());
    }

    @Test
    public void testGetLoadTimeoutMsWithTimeoutInCommonProperties() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addCommonProperties(createMap("timeout", "120"))
                .build();

        Table table = createTable(properties);

        // Timeout from common properties is 120 seconds = 120000 ms
        assertEquals(120 * 1000, table.getLoadTimeoutMs());
    }

    @Test
    public void testGetLoadParameters() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addProperty("key1", "value1")
                .build();

        Table table = createTable(properties);

        Map<String, String> params = table.getLoadParameters();
        assertEquals("value1", params.get("key1"));
        assertEquals("db", params.get("db"));
        assertEquals("table", params.get("table"));
    }

    @Test
    public void testIsMergeCommitAsyncDefault() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        Table table = createTable(properties);

        // Default should be false
        assertFalse(table.isMergeCommitAsync());
    }

    @Test
    public void testIsMergeCommitAsyncTrue() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addProperty("merge_commit_async", "true")
                .build();

        Table table = createTable(properties);

        assertTrue(table.isMergeCommitAsync());
    }

    @Test
    public void testGetDatabaseAndTable() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("test_db")
                .table("test_table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        Table table = createTable(properties);

        assertEquals("test_db", table.getDatabase());
        assertEquals("test_table", table.getTable());
    }

    @Test
    public void testGetProperties() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        Table table = createTable(properties);

        assertEquals(properties, table.getProperties());
    }
}
