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

package com.starrocks.flink.cdc.sync.starrocks;

import com.starrocks.flink.cdc.sync.table.Schema;
import com.starrocks.flink.cdc.sync.table.Table;

import java.util.HashMap;
import java.util.Map;

public class StarRocksTable extends Table {

    private static final long serialVersionUID = 1L;

    // -1 indicates unknown
    private int numBuckets = -1;
    private final Map<String, String> properties;

    public StarRocksTable(String databaseName, String tableName, Schema schema) {
        super(databaseName, tableName, schema);
        this.properties = new HashMap<>();
    }

    public void setNumBuckets(int numBuckets) {
        this.numBuckets = numBuckets;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public void addProperty(String key, String value) {
        properties.put(key, value);
    }

    public void addProperties(Map<String, String> newProperties) {
        properties.putAll(newProperties);
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
