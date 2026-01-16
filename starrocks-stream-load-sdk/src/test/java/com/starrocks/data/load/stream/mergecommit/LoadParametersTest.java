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
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LoadParametersTest {

    private Map<String, String> createMap(String... keyValues) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    @Test
    public void testConstants() {
        assertEquals("timeout", LoadParameters.TIMEOUT);
        assertEquals(600, LoadParameters.DEFAULT_TIMEOUT_SECONDS);
        assertEquals("enable_merge_commit", LoadParameters.ENABLE_MERGE_COMMIT);
        assertEquals("merge_commit_async", LoadParameters.MERGE_COMMIT_ASYNC);
    }

    @Test
    public void testGetParametersWithDefaultTimeout() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        Map<String, String> parameters = LoadParameters.getParameters(properties);

        // Should contain default timeout
        assertEquals(String.valueOf(LoadParameters.DEFAULT_TIMEOUT_SECONDS), parameters.get(LoadParameters.TIMEOUT));
    }

    @Test
    public void testGetParametersWithCustomTimeout() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addProperty("timeout", "300")
                .build();

        Map<String, String> parameters = LoadParameters.getParameters(properties);

        // Should use custom timeout
        assertEquals("300", parameters.get(LoadParameters.TIMEOUT));
    }

    @Test
    public void testGetParametersWithCommonProperties() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addCommonProperties(createMap("key1", "value1", "key2", "value2"))
                .build();

        Map<String, String> parameters = LoadParameters.getParameters(properties);

        // Should contain common properties
        assertEquals("value1", parameters.get("key1"));
        assertEquals("value2", parameters.get("key2"));
    }

    @Test
    public void testGetParametersWithPropertiesOverrideCommon() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addCommonProperties(createMap("key1", "common_value"))
                .addProperty("key1", "specific_value")
                .build();

        Map<String, String> parameters = LoadParameters.getParameters(properties);

        // Specific properties should override common properties
        assertEquals("specific_value", parameters.get("key1"));
    }

    @Test
    public void testGetParametersWithTimeoutInCommonProperties() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addCommonProperties(createMap("timeout", "120"))
                .build();

        Map<String, String> parameters = LoadParameters.getParameters(properties);

        // Should use timeout from common properties
        assertEquals("120", parameters.get(LoadParameters.TIMEOUT));
    }

    @Test
    public void testGetParametersWithCsvLz4Compression() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.CSV)
                .addProperty("compression", "lz4_frame")
                .build();

        Map<String, String> parameters = LoadParameters.getParameters(properties);

        // Should convert CSV compression to format=lz4
        assertEquals("lz4", parameters.get("format"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetParametersWithCsvUnsupportedCompression() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.CSV)
                .addProperty("compression", "gzip")
                .build();

        // Should throw UnsupportedOperationException
        LoadParameters.getParameters(properties);
    }

    @Test
    public void testGetParametersContainsDbAndTable() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("test_db")
                .table("test_table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        Map<String, String> parameters = LoadParameters.getParameters(properties);

        // Should contain db and table
        assertEquals("test_db", parameters.get("db"));
        assertEquals("test_table", parameters.get("table"));
    }
}
