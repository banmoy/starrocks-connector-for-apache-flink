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

import com.starrocks.flink.cdc.sync.table.Column;
import com.starrocks.flink.cdc.sync.table.Schema;
import com.starrocks.flink.cdc.sync.table.TableId;
import com.starrocks.flink.cdc.sync.utils.TableNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.starrocks.flink.cdc.sync.utils.JdbcUtils.createJdbcConnection;

public class StarRocksCatalog implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCatalog.class);

    private static final long serialVersionUID = 1L;

    private final String jdbcUrl;
    private final String userName;
    private final String password;

    // TODO no need to keep the connection open for long time
    private transient Connection connection;

    public StarRocksCatalog(String jdbcUrl, String userName, String password) {
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    public void open() throws Exception {
        if (connection == null) {
            this.connection = createJdbcConnection(jdbcUrl, userName, password);
        }
    }

    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    private Connection getConnection() {
        return connection;
    }

    public void createDataBase(String db, boolean ignoreIfExists) throws Exception {
        if (ignoreIfExists) {
            executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s;", db));
        } else {
            executeSql(String.format("CREATE DATABASE %s;", db));
        }
    }

    public void createTable(StarRocksTable table, boolean ignoreIfExists) throws Exception {
        StringBuilder builder = new StringBuilder();
        TableId tableId = table.getTableId();
        builder.append(
            String.format("CREATE TABLE %s`%s`.`%s`",
                ignoreIfExists ? "IF NOT EXISTS " : "",
                tableId.getDatabaseName(),
                tableId.getTableName())
        );
        builder.append(" (\n");
        Schema schema = table.getSchema();
        String columnsStmt = schema.getColumns().stream().map(this::buildColumnStatement)
                .collect(Collectors.joining(",\n"));
        builder.append(columnsStmt);
        builder.append("\n) ");
        String primaryKeys = schema.getPrimaryKeyColumnNames().stream()
                .map(pk -> "`" + pk + "`").collect(Collectors.joining(", "));
        builder.append(String.format("PRIMARY KEY (%s)\n", primaryKeys));
        builder.append(String.format("DISTRIBUTED BY HASH (%s)", primaryKeys));
        if (table.getNumBuckets() > 0) {
            builder.append(" BUCKETS ");
            builder.append(table.getNumBuckets());
        }
        if (!table.getProperties().isEmpty()) {
            builder.append("\nPROPERTIES (\n");
            String properties = table.getProperties().entrySet().stream()
                    .map(entry -> String.format("\"%s\" = \"%s\"", entry.getKey(), entry.getValue()))
                    .collect(Collectors.joining(",\n"));
            builder.append(properties);
            builder.append("\n)");
        }
        builder.append(";");

        String ddl = builder.toString();
        executeSql(ddl);
    }

    private String buildColumnStatement(Column column) {
        StringBuilder builder = new StringBuilder();
        builder.append("`");
        builder.append(column.getName());
        builder.append("` ");
        builder.append(getFullColumnType(column.getType(), column.getSize(), column.getScale()));
        builder.append(" ");
        builder.append(column.isNullable() ? "NULL" : "NOT NULL");
        if (column.getDefaultValue() != null) {
            builder.append(" DEFAULT '");
            builder.append(column.getDefaultValue());
            builder.append("'");
        }
        if (column.getComment() != null) {
            builder.append(" COMMENT \"");
            builder.append(column.getComment());
            builder.append("\"");
        }
        return builder.toString();
    }

    private String getFullColumnType(String type, @Nullable Integer size, @Nullable Integer scale) {
        String dataType = type.toUpperCase();
        switch (dataType) {
            case "DECIMAL":
                return String.format("DECIMAL(%d, %s)", size, scale);
            case "CHAR":
            case "VARCHAR":
                return String.format("%s(%d)", dataType, size);
            default:
                return dataType;
        }
    }

    public StarRocksTable getTable(String database, String table) throws TableNotExistException, Exception {
        Schema schema = getSchema(database, table);
        if (schema == null) {
            throw new TableNotExistException(database, table);
        }
        return new StarRocksTable(database, table, schema);
    }

    private static final String TABLE_SCHEMA_QUERY =
            "SELECT `COLUMN_NAME`, `DATA_TYPE`, `ORDINAL_POSITION`, `COLUMN_SIZE`, `DECIMAL_DIGITS`, `COLUMN_DEFAULT`, `IS_NULLABLE`, `COLUMN_KEY`" +
                    " FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA`=? AND `TABLE_NAME`=?;";

    // Return null if the table does not exist.
    @Nullable
    private Schema getSchema(String database, String table) throws Exception {
        List<Column> columns = new ArrayList<>();
        SortedMap<Integer, String> pkSortedMap = new TreeMap<>();
        try (PreparedStatement statement = getConnection().prepareStatement(TABLE_SCHEMA_QUERY)) {
            statement.setObject(1, database);
            statement.setObject(2, table);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String name = resultSet.getString("COLUMN_NAME");
                    String type = resultSet.getString("DATA_TYPE");
                    int position = resultSet.getInt("ORDINAL_POSITION");
                    Integer size = resultSet.getInt("COLUMN_SIZE");
                    if (resultSet.wasNull()) {
                        size = null;
                    }
                    Integer scale = resultSet.getInt("DECIMAL_DIGITS");
                    if (resultSet.wasNull()) {
                        scale = null;
                    }
                    String defaultValue = resultSet.getString("COLUMN_DEFAULT");
                    String isNullable = resultSet.getString("IS_NULLABLE");

                    String columnKey = resultSet.getString("COLUMN_KEY");
                    if (columnKey != null && columnKey.equalsIgnoreCase("PRI")) {
                        // the order of primary keys is the same as that in the schema
                        pkSortedMap.put(position, name);
                    }

                    Column column = new Column.Builder()
                            .setName(name)
                            .setOrdinalPosition(position)
                            .setType(type)
                            .setSize(size)
                            .setScale(scale)
                            .setDefaultValue(defaultValue)
                            .setNullable(isNullable == null || !isNullable.equalsIgnoreCase("NO"))
                            .setComment(null)
                            .build();

                    columns.add(column);
                }
            }
        }

        return columns.isEmpty() ? null : new Schema(columns, new ArrayList<>(pkSortedMap.values()));
    }

    public void executeSql(String sql) throws Exception {
        LOG.info("Execute sql: \n{}", sql);
        // TODO how to deal with the exception
        LOG.info("Execute sql: {}", sql);
        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.execute();
        }
    }
}
