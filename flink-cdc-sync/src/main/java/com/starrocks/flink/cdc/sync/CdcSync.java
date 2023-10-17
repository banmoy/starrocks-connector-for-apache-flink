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

import com.starrocks.flink.cdc.sync.utils.ConfigurationUtils;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

public class CdcSync {

    public static void main(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        if (params.has("help")) {
            printHelp();
            return;
        }

        String srcType = ConfigurationUtils.getRequiredValue(params, "src-type");
        Preconditions.checkArgument("mysql".equalsIgnoreCase(srcType));
        SyncDatabase syncDatabase = new SyncDatabase(
                ConfigurationUtils.getRequiredValue(params, SyncOptions.STARROCKS_DATABASE.key()),
                ConfigurationUtils.optionalConfigMap(params, "src-conf"),
                ConfigurationUtils.optionalConfigMap(params, "sink-conf"),
                ConfigurationUtils.optionalConfigMap(params, "table-conf"),
                getDataBaseSyncOptions(params)
            );
        syncDatabase.build();
    }

    private static Map<String, String> getDataBaseSyncOptions(MultipleParameterTool params) {
        Map<String, String> map = new HashMap<>();
        String includingTables = params.get(SyncOptions.INCLUDING_TABLES.key(), null);
        if (includingTables != null) {
            map.put(SyncOptions.INCLUDING_TABLES.key(), includingTables);
        }

        String excludingTables = params.get(SyncOptions.EXCLUDING_TABLES.key(), null);
        if (excludingTables != null) {
            map.put(SyncOptions.EXCLUDING_TABLES.key(), excludingTables);
        }

        return map;
    }

    private static void printHelp() {
        throw new UnsupportedOperationException();
    }

}
