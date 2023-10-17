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

package com.starrocks.flink.cdc.sync;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class SyncOptions {

    // For database sync
    public static final ConfigOption<String> STARROCKS_DATABASE =
            ConfigOptions.key("starrocks-db").stringType().noDefaultValue();
    public static final ConfigOption<String> INCLUDING_TABLES =
            ConfigOptions.key("including-tables").stringType().noDefaultValue();
    public static final ConfigOption<String> EXCLUDING_TABLES =
            ConfigOptions.key("excluding-tables").stringType().noDefaultValue();

    public static final ConfigOption<Integer> NUM_BUCKETS =
            ConfigOptions.key("num-buckets").intType().defaultValue(-1);
}
