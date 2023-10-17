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

package com.starrocks.flink.cdc.sync.table;

import java.io.Serializable;
import java.util.Objects;

public class TableId implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String databaseName;
    private final String tableName;

    public TableId(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableId tableId = (TableId) o;
        return Objects.equals(databaseName, tableId.databaseName) && Objects.equals(tableName, tableId.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName);
    }

    @Override
    public String toString() {
        return "TableId{" +
                "databaseName='" + databaseName + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }

    public static TableId of(String db, String table) {
        return new TableId(db, table);
    }
}
