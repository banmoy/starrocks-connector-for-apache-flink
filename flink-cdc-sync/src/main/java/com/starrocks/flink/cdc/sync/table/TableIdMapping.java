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

/** Map a source table id to the target StarRocks table id. */
public interface TableIdMapping extends Serializable {

    TableId map(TableId sourceTableId);

    class SingleDataBaseSync implements TableIdMapping {

        private static final long serialVersionUID = 1L;

        private final String targetDataBase;

        public SingleDataBaseSync(String targetDataBase) {
            this.targetDataBase = targetDataBase;
        }

        @Override
        public TableId map(TableId sourceTableId) {
            return TableId.of(targetDataBase, sourceTableId.getTableName());
        }
    }

    class SingleTableSync implements TableIdMapping {

        private static final long serialVersionUID = 1L;

        private final TableId targetTableId;

        public SingleTableSync(TableId targetTableId) {
            this.targetTableId = targetTableId;
        }

        @Override
        public TableId map(TableId sourceTableId) {
            return targetTableId;
        }
    }
}
