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

import com.starrocks.connector.flink.row.sink.StarRocksSinkOP;
import com.starrocks.flink.cdc.sync.table.Column;
import com.starrocks.flink.cdc.sync.table.Schema;
import com.starrocks.flink.cdc.sync.table.TableId;
import com.starrocks.flink.cdc.sync.table.TableIdMapping;
import com.starrocks.flink.cdc.sync.utils.MysqlUtils;
import com.starrocks.flink.cdc.sync.utils.StarRocksException;
import com.starrocks.shade.com.alibaba.fastjson.JSON;
import com.starrocks.shade.com.alibaba.fastjson.JSONArray;
import com.starrocks.shade.com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import io.debezium.relational.history.TableChanges;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.DEBEZIUM_TIME_DATE;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.DEBEZIUM_TIME_MICRO_TIMESTAMP;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.DEBEZIUM_TIME_NANO_TIMESTAMP;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.DEBEZIUM_TIME_PREFIX;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.DEBEZIUM_TIME_TIMESTAMP;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.PAYLOAD;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.PAYLOAD_AFTER;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.PAYLOAD_BEFORE;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.PAYLOAD_OP;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.PAYLOAD_SOURCE;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.PAYLOAD_SOURCE_DB;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.PAYLOAD_SOURCE_TABLE;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.SCHEMA;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.SCHEMA_FIELD;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.SCHEMA_FIELDS;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.SCHEMA_FIELD_NAME;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.SCHEMA_FIELD_TYPE;
import static com.starrocks.flink.cdc.sync.utils.DebeziumJsonUtils.SCHEMA_FIELD_TYPE_STRING;

// Refer to https://github.com/thimoonxy/flink-sync/blob/master/src/main/java/com/starrocks/CDCJob.java
public class DebeziumJsonEventProcessor extends RichFlatMapFunction<String, StarRocksRowWithShuffleKey> {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumJsonEventProcessor.class);

    private static final long serialVersionUID = 1L;

    private final StarRocksCatalog starRocksCatalog;
    private final TableIdMapping tableIdMapping;
    private final CreateTableConf createTableConf;
    private transient Map<TableId, StarRocksTable> cachedTables;
    private transient Map<TableId, List<JSONObject>> tableTimeCols;

    public DebeziumJsonEventProcessor(StarRocksCatalog starRocksCatalog, TableIdMapping tableIdMapping, CreateTableConf createTableConf) {
        this.starRocksCatalog = starRocksCatalog;
        this.tableIdMapping = tableIdMapping;
        this.createTableConf = createTableConf;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.starRocksCatalog.open();
        this.cachedTables = new HashMap<>();
        this.tableTimeCols = new HashMap<>();
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.starRocksCatalog.close();
    }

    @Override
    public void flatMap(String value, Collector<StarRocksRowWithShuffleKey> collector) throws Exception {
        JSONObject root = JSON.parseObject(value);
        JSONObject schema = root.getJSONObject(SCHEMA);
        JSONObject payload = root.getJSONObject(PAYLOAD);

        if (payload.get("op") == null) {
            String historyRecordStr = payload.getString("historyRecord");
            if (historyRecordStr == null) {
                return;
            }

            String srcDb = payload.getJSONObject(PAYLOAD_SOURCE).getString(PAYLOAD_SOURCE_DB);
            String srcTable = payload.getJSONObject(PAYLOAD_SOURCE).getString(PAYLOAD_SOURCE_TABLE);
            JSONObject historyRecord = JSON.parseObject(historyRecordStr);
            JSONArray tableChanges = historyRecord.getJSONArray("tableChanges");
            JSONObject tableChange = tableChanges.getJSONObject(0);
            String changeType = tableChange.getString("type");
            JSONObject table = tableChange.getJSONObject("table");
            if (changeType.equals(TableChanges.TableChangeType.CREATE.name())) {
                parseCreateTable(srcDb, srcTable, table);
            } else if (changeType.equals(TableChanges.TableChangeType.ALTER.name())) {
                parseSchemaChange(srcDb, srcTable, table);
            }
        } else {
            parseData(schema, payload, collector);
        }
    }

    private void parseCreateTable(String srcDb, String srcTable, JSONObject table) throws Exception {
        JSONArray columns = table.getJSONArray("columns");
        if (columns == null) {
            return;
        }
        List<Column> mysqlColumns = new ArrayList<>();
        List<String> primaryKeyColumnNames = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            JSONObject column = columns.getJSONObject(i);
            Column.Builder builder = new Column.Builder()
                    .setName(column.getString("name"))
                    .setOrdinalPosition(column.getInteger("position"))
                    .setType(column.getString("typeName"))
                    .setSize(column.getInteger("length"))
                    .setScale(column.getInteger("scale"))
                    .setDefaultValue(column.getString("defaultValueExpression"))
                    .setNullable(column.getBoolean("optional"))
                    .setComment(column.getString("comment"));
            mysqlColumns.add(builder.build());
        }
        JSONArray primaryKeys = table.getJSONArray("primaryKeyColumnNames");
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            return;
        }
        for (int i = 0; i < primaryKeys.size(); i++) {
            primaryKeyColumnNames.add(primaryKeys.getString(i));
        }
        Schema mysqlSchema = new Schema(mysqlColumns, primaryKeyColumnNames);
        Schema starRocksSchema = MysqlUtils.toStarRocksSchema(mysqlSchema);
        TableId targetTableId = tableIdMapping.map(TableId.of(srcDb, srcTable));
        StarRocksTable starRocksTable = new StarRocksTable(
                targetTableId.getDatabaseName(), targetTableId.getTableName(), starRocksSchema);
        CreateTableConf.setStarRocksTable(starRocksTable, createTableConf);
        starRocksCatalog.createTable(starRocksTable, false);
    }

    private void parseSchemaChange(String srcDb, String srcTable, JSONObject table) throws Exception {
        JSONArray columns = table.getJSONArray("columns");
        if (columns == null) {
            return;
        }
        TableId targetTableId = tableIdMapping.map(TableId.of(srcDb, srcTable));
        StarRocksTable starRocksTable = getStarRocksTable(targetTableId);
        List<Column> mysqlColumns = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            JSONObject column = columns.getJSONObject(i);
            Column.Builder builder = new Column.Builder()
                    .setName(column.getString("name"))
                    .setOrdinalPosition(column.getInteger("position"))
                    .setType(column.getString("typeName"))
                    .setSize(column.getInteger("length"))
                    .setScale(column.getInteger("scale"))
                    .setDefaultValue(column.getString("defaultValueExpression"))
                    .setNullable(column.getBoolean("optional"))
                    .setComment(column.getString("comment"));
            mysqlColumns.add(builder.build());
        }
        // No need to know the primary key for schema change
        Schema mysqlSchema = new Schema(mysqlColumns, Collections.emptyList());
        Schema newStarRocksSchema = MysqlUtils.toStarRocksSchema(mysqlSchema);
        Schema oldStarRocksSchema = starRocksTable.getSchema();
        List<Column> addColumns = new ArrayList<>();
        List<Column> dropColumns = new ArrayList<>();
        for (Column newColumn : newStarRocksSchema.getColumns()) {
            if (oldStarRocksSchema.getColumn(newColumn.getName()) == null) {
                addColumns.add(newColumn);
            }
        }
        for (Column oldColumn : oldStarRocksSchema.getColumns()) {
            if (newStarRocksSchema.getColumn(oldColumn.getName()) == null) {
                dropColumns.add(oldColumn);
            }
        }

        StarRocksTable newTable = null;
        if (!addColumns.isEmpty()) {
            String addClause = addColumns.stream()
                    .map(column -> String.format("ADD COLUMN `%s` %s", column.getName(), column.getType()))
                    .collect(Collectors.joining(", "));
            String addColumnSql = String.format("ALTER TABLE `%s`.`%s` %s", starRocksTable.getTableId().getDatabaseName(),
                    starRocksTable.getTableId().getTableName(), addClause);
            List<String> columnNames = addColumns.stream().map(Column::getName).collect(Collectors.toList());
            newTable = executeSchemaChange(starRocksTable.getTableId(), addColumnSql, columnNames, true);
        }

        if (!dropColumns.isEmpty()) {
            String dropClause = dropColumns.stream()
                    .map(column -> String.format("DROP COLUMN `%s`", column.getName()))
                    .collect(Collectors.joining(", "));
            String dropColumnSql = String.format("ALTER TABLE `%s`.`%s` %s", starRocksTable.getTableId().getDatabaseName(),
                    starRocksTable.getTableId().getTableName(), dropClause);
            List<String> columnNames = addColumns.stream().map(Column::getName).collect(Collectors.toList());
            newTable = executeSchemaChange(starRocksTable.getTableId(), dropColumnSql, columnNames, false);
        }

        if (newTable != null) {
            cachedTables.put(newTable.getTableId(), newTable);
            tableTimeCols.remove(newTable.getTableId());
        }
    }

    private void parseData(JSONObject schema, JSONObject payload, Collector<StarRocksRowWithShuffleKey> collector) throws Exception {
        String srcDb = payload.getJSONObject(PAYLOAD_SOURCE).getString(PAYLOAD_SOURCE_DB);
        String srcTable = payload.getJSONObject(PAYLOAD_SOURCE).getString(PAYLOAD_SOURCE_TABLE);
        TableId targetTableId = tableIdMapping.map(TableId.of(srcDb, srcTable));
        StarRocksStringRow rowData = new StarRocksStringRow(targetTableId.getDatabaseName(), targetTableId.getTableName());
        String op = payload.getString(PAYLOAD_OP);
        JSONObject row;
        if (Envelope.Operation.CREATE.code().equals(op) || Envelope.Operation.READ.code().equals(op)) {
            row = transformRow(schema, payload, PAYLOAD_AFTER, targetTableId);
            row.put(StarRocksSinkOP.COLUMN_KEY, StarRocksSinkOP.UPSERT.ordinal());
            rowData.setRow(JSON.toJSONString(row));
        } else if (Envelope.Operation.DELETE.code().equals(op)) {
            row = transformRow(schema, payload, PAYLOAD_BEFORE, targetTableId);
            row.put(StarRocksSinkOP.COLUMN_KEY, StarRocksSinkOP.DELETE.ordinal());
            rowData.setRow(JSON.toJSONString(row));
        } else {
            row = transformRow(schema, payload, PAYLOAD_AFTER, targetTableId);
            row.put(StarRocksSinkOP.COLUMN_KEY, StarRocksSinkOP.UPSERT.ordinal());
            rowData.setRow(JSON.toJSONString(row));
        }

        StarRocksTable table = getStarRocksTable(targetTableId);
        List<String> primaryKeyColumnNames = table.getSchema().getPrimaryKeyColumnNames();
        List<String> primaryKeyValues = new ArrayList<>(primaryKeyColumnNames.size());
        for (String name : primaryKeyColumnNames) {
            primaryKeyValues.add(row.get(name).toString());
        }
        StarRocksRowWithShuffleKey rowWithShuffleKey = new StarRocksRowWithShuffleKey(rowData, primaryKeyValues);
        collector.collect(rowWithShuffleKey);
    }

    private JSONObject transformRow(JSONObject schema, JSONObject payload, String beforeOrAfter, TableId targetTableId) {
        JSONObject row = payload.getJSONObject(beforeOrAfter);
        tableTimeCols.computeIfAbsent(
                targetTableId,
                k -> schema.getJSONArray(SCHEMA_FIELDS)
                        .stream()
                        .filter(obj ->  beforeOrAfter.equals(((JSONObject)obj).getString(SCHEMA_FIELD)))
                        .flatMap(obj -> ((JSONObject)obj).getJSONArray(SCHEMA_FIELDS).stream().map(o -> (JSONObject)o))
                        .filter(obj -> obj.containsKey(SCHEMA_FIELD_NAME) && obj.getString(SCHEMA_FIELD_NAME).startsWith(DEBEZIUM_TIME_PREFIX)
                                &&  !SCHEMA_FIELD_TYPE_STRING.equals(obj.getString(SCHEMA_FIELD_TYPE)))
                        .collect(Collectors.toList())
        ).forEach(obj -> {
            String fieldName = obj.getString(SCHEMA_FIELD);
            if ( null == row.getLong(fieldName)){
                return; // in case value is null
            }
            switch (obj.getString(SCHEMA_FIELD_NAME).replace(DEBEZIUM_TIME_PREFIX, "")) {
                case DEBEZIUM_TIME_TIMESTAMP:
                    row.put(fieldName, TimestampData.fromEpochMillis(row.getLong(fieldName)).toString());
                    break;
                case DEBEZIUM_TIME_MICRO_TIMESTAMP:
                    long micro = row.getLong(fieldName);
                    row.put(fieldName, TimestampData.fromEpochMillis(micro / 1000, (int) (micro % 1000 * 1000)).toString());
                    break;
                case DEBEZIUM_TIME_NANO_TIMESTAMP:
                    long nano = row.getLong(fieldName);
                    row.put(fieldName, TimestampData.fromEpochMillis(nano / 1000_000, (int) (nano % 1000_000)).toString());
                    break;
                case DEBEZIUM_TIME_DATE:
                    row.put(fieldName, TemporalConversions.toLocalDate(row.getLong(fieldName)).toString());
                    break;
            }
        });
        return row;
    }

    private StarRocksTable executeSchemaChange(TableId tableId, String schemaChangeSql, List<String> alterColumns, boolean isAdd) throws Exception {
        try {
            starRocksCatalog.executeSql(schemaChangeSql);
        } catch (Exception e) {
            LOG.error("Failed to execute ddl", e);
            throw e;
        }

        long startTime = System.currentTimeMillis();
        // TODO make timeout configurable
        long timeoutMs = 600000;
        do {
            try {
                StarRocksTable newTable = starRocksCatalog.getTable(tableId.getDatabaseName(), tableId.getTableName());
                Schema newSchema = newTable.getSchema();
                boolean allMatch = true;
                for (String col : alterColumns) {
                    Column column = newSchema.getColumn(col);
                    if ((isAdd && column == null) || (!isAdd && column != null)) {
                        allMatch = false;
                        break;
                    }
                }
                if (allMatch) {
                    return newTable;
                }
            } catch (Exception e) {
                LOG.error("Failed to get table", e);
                throw e;
            }
            try {
                Thread.sleep(10000);
            } catch (Exception e) {
                LOG.error("Failed to sleep", e);
                throw e;
            }
        } while (System.currentTimeMillis() - startTime < timeoutMs);

        throw new StarRocksException("Timeout to execute schema change");
    }

    private StarRocksTable getStarRocksTable(TableId tableId) throws Exception {
        StarRocksTable table = cachedTables.get(tableId);
        if (table == null) {
            table = starRocksCatalog.getTable(tableId.getDatabaseName(), tableId.getTableName());
            cachedTables.put(table.getTableId(), table);
        }
        return table;
    }
}
