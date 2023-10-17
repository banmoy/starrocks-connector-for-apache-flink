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

import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import static com.starrocks.flink.cdc.sync.utils.JdbcUtils.createJdbcConnection;

public class MysqlTypeTest {

    @Test
    public void testTypes() throws Exception {
        try (Connection conn = createJdbcConnection("jdbc:mysql://127.0.0.1:3306", "root", "12345678")) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet schemas = metaData.getSchemas()) {
                while (schemas.next()) {
                    String database = schemas.getString("TABLE_CATALOG");
                    String schema = schemas.getString("TABLE_SCHEM");
                    try (ResultSet tables = metaData.getTables(database, null, "%", null)) {
                        while (tables.next()) {
                            String table = tables.getString("TABLE_NAME");
                            System.out.println(database + " " + schema + " " + table);
                        }
                    }
                }
            }
        }
    }
}
