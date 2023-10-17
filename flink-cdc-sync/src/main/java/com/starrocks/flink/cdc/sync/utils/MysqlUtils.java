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

import com.starrocks.flink.cdc.sync.table.Column;
import com.starrocks.flink.cdc.sync.table.Schema;
import com.starrocks.flink.cdc.sync.table.Table;
import com.starrocks.flink.cdc.sync.starrocks.StarRocksTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;

import static com.starrocks.flink.cdc.sync.utils.JdbcUtils.createJdbcConnection;

public class MysqlUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlUtils.class);

    public static List<Table> getTables(String jdbcUrl, String username, String password,
                                        Predicate<String> dbPredicate, Predicate<String> tablePredicate) throws Exception {
        List<Table> tableList = new ArrayList<>();
        try (Connection conn = createJdbcConnection(jdbcUrl, username, password)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet catalogs = metaData.getCatalogs()) {
                while (catalogs.next()) {
                    String database = catalogs.getString("TABLE_CAT");
                    if (!dbPredicate.test(database)) {
                        LOG.info("Exclude database {}", database);
                        continue;
                    }
                    try (ResultSet tables = metaData.getTables(database, null, "%", null)) {
                        while (tables.next()) {
                            String table = tables.getString("TABLE_NAME");
                            if (!tablePredicate.test(table)) {
                                LOG.info("Exclude table {}.{}", database, table);
                                continue;
                            }
                            Schema schema = getTableSchema(metaData, database, table);
                            tableList.add(new Table(database, table, schema));
                            LOG.info("Include table {}.{}", database, table);
                        }
                    }
                }
            }
        }
        return tableList;
    }

    public static Schema getTableSchema(DatabaseMetaData metaData, String database, String table) throws SQLException {
        List<Column> columns = new ArrayList<>();
        try (ResultSet rs = metaData.getColumns(database, null, table, null)) {
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                int position = rs.getInt("ORDINAL_POSITION");
                String type = rs.getString("TYPE_NAME");
                Integer size = rs.getInt("COLUMN_SIZE");
                if (rs.wasNull()) {
                    size = null;
                }
                Integer scale = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    scale = null;
                }
                String defaultValue = rs.getString("COLUMN_DEF");
                String isNullable = rs.getString("IS_NULLABLE");
                String comment = rs.getString("REMARKS");

                Column column = new Column.Builder()
                        .setName(name)
                        .setOrdinalPosition(position)
                        .setType(type)
                        .setSize(size)
                        .setScale(scale)
                        .setDefaultValue(defaultValue)
                        .setNullable(isNullable == null || !isNullable.equalsIgnoreCase("NO"))
                        .setComment(comment)
                        .build();

                columns.add(column);
            }
        }

        // map KEY_SEQ to pk column name
        SortedMap<Integer, String> pkSortedMap = new TreeMap<>();
        try (ResultSet rs = metaData.getPrimaryKeys(database, null, table)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                short keySeq = rs.getShort("KEY_SEQ");
                pkSortedMap.put((int) keySeq, columnName);
            }
        }

        return new Schema(columns, new ArrayList<>(pkSortedMap.values()));
    }

    public static Schema toStarRocksSchema(Schema mysqlSchema) {
        List<Column> starRocksColumns = new ArrayList<>();
        for (Column mysqlColumn : mysqlSchema.getColumns()) {
            starRocksColumns.add(toStarRocksColumn(mysqlColumn));
        }

        // move primary key columns to the head positions
        Map<String, Column> columnMap = new HashMap<>();
        starRocksColumns.forEach(column -> columnMap.put(column.getName(), column));
        List<String> primaryKeyColumnNames = mysqlSchema.getPrimaryKeyColumnNames();
        List<Column> orderedColumns = new ArrayList<>();
        primaryKeyColumnNames.forEach(name -> orderedColumns.add(columnMap.get(name)));
        starRocksColumns.stream().filter(column -> !primaryKeyColumnNames.contains(column.getName()))
                .forEach(orderedColumns::add);

        List<Column> finalColumns = new ArrayList<>();
        for (int i = 0; i < orderedColumns.size(); i++) {
            Column column = orderedColumns.get(i);
            if (column.getOrdinalPosition() == i + 1) {
                finalColumns.add(column);
            } else {
                Column newColumn = new Column.Builder()
                        .setName(column.getName())
                        // use the ordered position
                        .setOrdinalPosition(i + 1)
                        .setType(column.getType())
                        .setSize(column.getSize())
                        .setScale(column.getScale())
                        .setDefaultValue(column.getDefaultValue())
                        .setNullable(column.isNullable())
                        .setComment(column.getComment())
                        .build();
                finalColumns.add(newColumn);
            }
        }
        return new Schema(finalColumns, primaryKeyColumnNames);
    }

    public static Column toStarRocksColumn(Column mysqlColumn) {
        Column.Builder builder = new Column.Builder()
                .setName(mysqlColumn.getName())
                .setOrdinalPosition(mysqlColumn.getOrdinalPosition())
                .setType(mysqlColumn.getType())
                .setSize(mysqlColumn.getSize())
                .setScale(mysqlColumn.getScale())
                .setDefaultValue(mysqlColumn.getDefaultValue())
                .setNullable(mysqlColumn.isNullable())
                .setComment(mysqlColumn.getComment());

        Integer size = mysqlColumn.getSize();
        Integer scale = mysqlColumn.getScale();
        String newType = mysqlColumn.getType();
        Integer newSize = size;
        Integer newScale = scale;
        switch (mysqlColumn.getType().toUpperCase()) {
            case "BOOL":
            case "BOOLEAN":
                newType = StarRocksTypes.BOOLEAN;
                break;
            case "BIT":
                // BIT(1) in mysql is boolean in general
                if (size == null || size == 1) {
                    newType = StarRocksTypes.BOOLEAN;
                } else {
                    throwUnsupportedTypeException(mysqlColumn,
                            "only support to convert MySQL BIT(1) to StarRocks BOOLEAN");
                }
                break;
            case "TINYINT":
                // TINYINT(1) in mysql is boolean in general
                if (size != null && size == 1) {
                    newType = StarRocksTypes.BOOLEAN;
                } else {
                    newType = StarRocksTypes.TINYINT;
                }
                break;
            case "TINYINT UNSIGNED":
            case "TINYINT UNSIGNED ZEROFILL":
            case "SMALLINT":
                newType = StarRocksTypes.SMALLINT;
                break;
            case "SMALLINT UNSIGNED":
            case "SMALLINT UNSIGNED ZEROFILL":
            case "MEDIUMINT":
            case "INT":
                newType = StarRocksTypes.INT;
                break;
            case "MEDIUMINT UNSIGNED":
            case "MEDIUMINT UNSIGNED ZEROFILL":
            case "INT UNSIGNED":
            case "INT UNSIGNED ZEROFILL":
            case "BIGINT":
                newType = StarRocksTypes.BIGINT;
                break;
            case "BIGINT UNSIGNED":
            case "BIGINT UNSIGNED ZEROFILL":
            case "SERIAL":
                newType = StarRocksTypes.LARGEINT;
                break;
            case "FLOAT":
            case "FLOAT UNSIGNED":
            case "FLOAT UNSIGNED ZEROFILL":
                newType = StarRocksTypes.FLOAT;
                break;
            case "REAL":
            case "REAL UNSIGNED":
            case "REAL UNSIGNED ZEROFILL":
            case "DOUBLE":
            case "DOUBLE UNSIGNED":
            case "DOUBLE UNSIGNED ZEROFILL":
            case "DOUBLE PRECISION":
            case "DOUBLE PRECISION UNSIGNED":
            case "DOUBLE PRECISION UNSIGNED ZEROFILL":
                newType = StarRocksTypes.DOUBLE;
                break;
            case "NUMERIC":
            case "NUMERIC UNSIGNED":
            case "NUMERIC UNSIGNED ZEROFILL":
            case "DECIMAL":
            case "DECIMAL UNSIGNED":
            case "DECIMAL UNSIGNED ZEROFILL":
            case "FIXED":
            case "FIXED UNSIGNED":
            case "FIXED UNSIGNED ZEROFILL":
                // StarRocks only support 38 precision
                if (size != null && size <= 38) {
                    newType = StarRocksTypes.DECIMAL;
                    newSize = size;
                    newScale = scale == null ? 0 : scale;
                } else {
                    newType = StarRocksTypes.STRING;
                    newSize = null;
                    newScale = null;
                }
                break;
            case "CHAR":
                if (size == null || size <= 0) {
                    throwUnsupportedTypeException(mysqlColumn, "MySQL CHAR size must be positive");
                }
                // MySQL and StarRocks use different units for the size. It's the number of
                // characters in MySQL, and the number of bytes in StarRocks. Specially, one
                // chinese character will use 3 bytes because it uses UTF-8. So if the size
                // of bytes for MySQL is larger than 255, we use VARCHAR. 255 is the max length
                // of CHAR in StarRocks.
                newSize = size * 3;
                if (newSize <= 255) {
                    newType = StarRocksTypes.CHAR;
                } else {
                    newType = StarRocksTypes.VARCHAR;
                }
                break;
            case "VARCHAR":
                if (size == null || size <= 0) {
                    throwUnsupportedTypeException(mysqlColumn, "MySQL VARCHAR size must be positive");
                }
                // MySQL and StarRocks use different units for the size. It's the number of
                // characters in MySQL, and the number of bytes in StarRocks. Specially, one
                // chinese character will use 3 bytes because it uses UTF-8. So if the size
                // of bytes for MySQL is larger than 255, we use VARCHAR.
                newType = StarRocksTypes.VARCHAR;
                // TODO what if the newSize is larger the the max length of VARCHAR in StarRocks,
                // which is 1048576 by default
                newSize = Math.min(1048576, size * 3);
                break;
            case "DATE":
                newType = StarRocksTypes.DATE;
                break;
            case "DATETIME":
            case "TIMESTAMP":
                newType = StarRocksTypes.DATETIME;
                newSize = null;
                break;
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
                newType = StarRocksTypes.STRING;
                newSize = null;
                break;
            case "ENUM":
                newType = StarRocksTypes.STRING;
                break;
            case "YEAR":
                newType = StarRocksTypes.INT;
                break;
            case "JSON":
                newType = StarRocksTypes.JSON;
                break;
            default:
                throwUnsupportedTypeException(mysqlColumn, " The MySQL type is not supported yet.");
        }

        builder.setType(newType);
        builder.setSize(newSize);
        builder.setScale(newScale);
        return builder.build();
    }

    private static void throwUnsupportedTypeException(Column mysqlColumn, String errMsg) {
        throw new StarRocksException(
                    String.format("Unsupported MySQL type, column name: %s, data type: %s, column size: %s," +
                        " scale: %s, errMsg: %s", mysqlColumn.getName(), mysqlColumn.getType(), mysqlColumn.getSize(),
                            mysqlColumn.getScale(), errMsg));
    }
}
