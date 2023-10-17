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

import com.starrocks.shade.com.alibaba.fastjson.JSON;
import com.starrocks.shade.com.alibaba.fastjson.JSONArray;
import com.starrocks.shade.com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.PAYLOAD;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.SCHEMA;

public class DebeziumJsonParseExample {

    public static void main(String[] args) throws Exception {
        MysqlParser mysqlParser = new MysqlParser();
        mysqlParser.parseCreateTable();
    }

    private static List<String> readTestFile(String dataFile) throws Exception {
        final URL url = DebeziumJsonParseExample.class.getClassLoader().getResource(dataFile);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    private static abstract class DebeziumJsonParseBase {

        private static final String INSERT_DATA_FILE = "insert.json";
        private static final String UPDATE_DATA_FILE = "update.json";
        private static final String DELETE_DATA_FILE = "delete.json";
        private static final String ADD_COLUMN_DATA_FILE = "add-column.json";
        private static final String DROP_COLUMN_DATA_FILE = "drop-column.json";
        private static final String MODIFY_COLUMN_DATA_FILE = "modify-column.json";
        private static final String RENAME_COLUMN_DATA_FILE = "rename-column.json";

        private static final String CREATE_TABLE_DATA_FILE = "create-table.json";

        public abstract String getDataDir();

        private JSONObject readAndParse(String dataFile) throws Exception {
            List<String> data = readTestFile(getDataDir() + "/" + dataFile);
            return JSONObject.parseObject(data.get(0));
        }

        public void parseInsert() throws Exception {
            JSONObject root = readAndParse(INSERT_DATA_FILE);
            JSONObject schema = root.getJSONObject(SCHEMA);
            JSONObject payload = root.getJSONObject(PAYLOAD);
        }

        public void parseUpdate() throws Exception {
            JSONObject root = readAndParse(UPDATE_DATA_FILE);
            JSONObject schema = root.getJSONObject(SCHEMA);
            JSONObject payload = root.getJSONObject(PAYLOAD);

        }

        public void parseDelete() throws Exception {
            JSONObject root = readAndParse(DELETE_DATA_FILE);
            JSONObject schema = root.getJSONObject(SCHEMA);
            JSONObject payload = root.getJSONObject(PAYLOAD);
        }

        public void parseAddColumn() throws Exception {
            JSONObject root = readAndParse(ADD_COLUMN_DATA_FILE);
            JSONObject schema = root.getJSONObject(SCHEMA);
            JSONObject payload = root.getJSONObject(PAYLOAD);
            String historyRecordValue = payload.getString("historyRecord");
            JSONObject historyRecord = JSON.parseObject(historyRecordValue);
            JSONArray tableChanges = historyRecord.getJSONArray("tableChanges");
            JSONArray columns = tableChanges.getJSONObject(0).getJSONObject("table").getJSONArray("columns");
        }

        public void parseDropColumn() throws Exception {
            JSONObject root = readAndParse(DROP_COLUMN_DATA_FILE);
            JSONObject schema = root.getJSONObject(SCHEMA);
            JSONObject payload = root.getJSONObject(PAYLOAD);
        }

        public void parseModifyColumn() throws Exception {
            JSONObject root = readAndParse(MODIFY_COLUMN_DATA_FILE);
            JSONObject schema = root.getJSONObject(SCHEMA);
            JSONObject payload = root.getJSONObject(PAYLOAD);
        }

        public void parseRenameColumn() throws Exception {
            JSONObject root = readAndParse(RENAME_COLUMN_DATA_FILE);
            JSONObject schema = root.getJSONObject(SCHEMA);
            JSONObject payload = root.getJSONObject(PAYLOAD);
        }

        public void parseCreateTable() throws Exception {
            JSONObject root = readAndParse(CREATE_TABLE_DATA_FILE);
            JSONObject schema = root.getJSONObject(SCHEMA);
            JSONObject payload = root.getJSONObject(PAYLOAD);
            String historyRecordValue = payload.getString("historyRecord");
            JSONObject historyRecord = JSON.parseObject(historyRecordValue);
            JSONArray tableChanges = historyRecord.getJSONArray("tableChanges");
            JSONObject table = tableChanges.getJSONObject(0).getJSONObject("table");
            JSONArray primaryKeys = table.getJSONArray("primaryKeyColumnNames");
            List<String> primaryKeyColumnNames = new ArrayList<>();
            for (int i = 0; i < primaryKeys.size(); i++) {
                primaryKeyColumnNames.add(primaryKeys.getString(i));
            }
            System.out.println(primaryKeyColumnNames);
        }
    }

    private static class MysqlParser extends DebeziumJsonParseBase {

        @Override
        public String getDataDir() {
            return "mysql-debezium-json";
        }
    }
}
