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

import com.starrocks.connector.flink.table.data.StarRocksRowData;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class StarRocksRowWithShuffleKey implements Serializable {

    private static final long serialVersionUID = 1L;

    private final StarRocksRowData rowData;
    private final List<String> primaryKeyValues;

    public StarRocksRowWithShuffleKey(StarRocksRowData rowData, List<String> primaryKeyValues) {
        this.rowData = rowData;
        this.primaryKeyValues = primaryKeyValues;
    }

    public StarRocksRowData getRowData() {
        return rowData;
    }

    public List<String> getPrimaryKeyValues() {
        return primaryKeyValues;
    }

    // Only hash db, table and primary keys
    @Override
    public int hashCode() {
        return Objects.hash(rowData.getDatabase(), rowData.getTable(), primaryKeyValues);
    }
}
