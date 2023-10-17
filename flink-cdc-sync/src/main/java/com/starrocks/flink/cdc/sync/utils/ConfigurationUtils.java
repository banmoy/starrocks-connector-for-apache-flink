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

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

// TODO some codes are copied from paimon, should refactor or make a statement
public class ConfigurationUtils {

    public static Map<String, String> getPrefixConfigs(String prefix, Configuration conf) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : conf.toMap().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                result.put(key.substring(prefix.length()), entry.getValue());
            }
        }
        return result;
    }

    public static Map<String, String> optionalConfigMap(MultipleParameterTool params, String key) {
        if (!params.has(key)) {
            return Collections.emptyMap();
        }

        Map<String, String> config = new HashMap<>();
        for (String kvString : params.getMultiParameter(key)) {
            parseKeyValueString(config, kvString);
        }
        return config;
    }

    public static void parseKeyValueString(Map<String, String> map, String kvString) {
        String[] kv = kvString.split("=", 2);
        if (kv.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid key-value string '%s'. Please use format 'key=value'",
                            kvString));
        }
        map.put(kv[0].trim(), kv[1].trim());
    }


    public static String getRequiredValue(MultipleParameterTool params, String key) {
        checkRequiredArgument(params, key);
        return params.get(key);
    }


    public static void checkRequiredArgument(MultipleParameterTool params, String key) {
        Preconditions.checkArgument(
                params.has(key), "Argument '%s' is required. Run '<action> --help' for help.", key);
    }
}
