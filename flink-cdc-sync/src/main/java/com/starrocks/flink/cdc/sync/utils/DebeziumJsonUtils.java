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

package com.starrocks.flink.cdc.sync.utils;

public class DebeziumJsonUtils {

    public static final String PAYLOAD_BEFORE = "before";
    public static final String PAYLOAD_AFTER = "after";
    public static final String PAYLOAD = "payload";
    public static final String PAYLOAD_SOURCE = "source";
    public static final String PAYLOAD_SOURCE_DB = "db";
    public static final String PAYLOAD_SOURCE_TABLE = "table";
    public static final String PAYLOAD_OP = "op";

    public static final String SCHEMA = "schema";
    public static final String SCHEMA_FIELDS = "fields";
    public static final String SCHEMA_FIELD = "field";
    public static final String SCHEMA_FIELD_TYPE = "type";
    public static final String SCHEMA_FIELD_TYPE_STRING = "string";
    public static final String SCHEMA_FIELD_NAME = "name";
    public static final String DEBEZIUM_TIME_PREFIX = "io.debezium.time.";
    public static final String DEBEZIUM_TIME_DATE = "Date";
    public static final String DEBEZIUM_TIME_TIMESTAMP = "Timestamp";
    public static final String DEBEZIUM_TIME_MICRO_TIMESTAMP = "MicroTimestamp";
    public static final String DEBEZIUM_TIME_NANO_TIMESTAMP = "NanoTimestamp";
}
