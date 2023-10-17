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

import com.starrocks.flink.cdc.sync.table.Column;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Schema implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<Column> columns;
    // Sorted by the sequence of the primary key declaration.
    private final List<String> primaryKeyColumnNames;
    // map column names to Column structures
    private final Map<String, Column> columnMap;

    // Note the primaryKeyColumnNames should have been sorted
    // by the sequence of the primary key declaration.
    public Schema(List<Column> columns, List<String> primaryKeyColumnNames) {
        this.columns = new ArrayList<>(columns);
        this.columns.sort(Comparator.comparingInt(Column::getOrdinalPosition));
        this.primaryKeyColumnNames = new ArrayList<>(primaryKeyColumnNames);
        this.columnMap = new HashMap<>();
        this.columns.forEach(column -> columnMap.put(column.getName(), column));
    }

    public Column getColumn(String columnName) {
        return columnMap.get(columnName);
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<String> getPrimaryKeyColumnNames() {
        return primaryKeyColumnNames;
    }
}
