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

import javax.annotation.Nullable;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class RegexUtils {

    public static Predicate<String> createDbPredicate(String dbRegex) {
        final Pattern pattern = Pattern.compile(dbRegex);
        return dbName -> pattern.matcher(dbName).matches();
    }

    public static Predicate<String> createTablePredicate(
            @Nullable String includingTablesRegex, @Nullable String excludingTablesRegex) {
        final Pattern includingPattern = includingTablesRegex == null ? null : Pattern.compile(includingTablesRegex);
        final Pattern excludingPattern = excludingTablesRegex == null ? null : Pattern.compile(excludingTablesRegex);
        return tableName -> {
            if (excludingPattern != null && excludingPattern.matcher(tableName).matches()) {
                return false;
            }

            return includingPattern == null || includingPattern.matcher(tableName).matches();
        };
    }

    // TODO verify the regex
    public static String buildTableListRegex(
            String dbRegex, @Nullable String includingTablesRegex, @Nullable String excludingTablesRegex) {
        String includingRegex = includingTablesRegex == null ? ".*" : includingTablesRegex;
        if (excludingTablesRegex == null) {
            return String.format("%s%s", dbRegex, includingRegex);
        } else {
            return String.format("%s\\.(?!(%s)(%s))", dbRegex, excludingTablesRegex, includingRegex);
        }
    }
}
