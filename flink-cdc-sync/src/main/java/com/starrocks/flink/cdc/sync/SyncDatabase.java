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

import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.SinkFunctionFactory;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.flink.cdc.sync.starrocks.CreateTableConf;
import com.starrocks.flink.cdc.sync.starrocks.DebeziumJsonEventProcessor;
import com.starrocks.flink.cdc.sync.starrocks.StarRocksCatalog;
import com.starrocks.flink.cdc.sync.starrocks.StarRocksRowWithShuffleKey;
import com.starrocks.flink.cdc.sync.starrocks.StarRocksTable;
import com.starrocks.flink.cdc.sync.table.Schema;
import com.starrocks.flink.cdc.sync.table.Table;
import com.starrocks.flink.cdc.sync.table.TableId;
import com.starrocks.flink.cdc.sync.table.TableIdMapping;
import com.starrocks.flink.cdc.sync.utils.JdbcUtils;
import com.starrocks.flink.cdc.sync.utils.MysqlUtils;
import com.starrocks.flink.cdc.sync.utils.RegexUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffsetBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import com.ververica.cdc.debezium.utils.JdbcUrlUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.ValidationException;
import org.apache.kafka.connect.json.JsonConverterConfig;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.starrocks.flink.cdc.sync.utils.ConfigurationUtils.getPrefixConfigs;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;

public class SyncDatabase {

    private final String starRocksDatabase;
    private final Configuration mysqlConf;
    private final Configuration sinkConf;
    private final CreateTableConf createTableConf;
    private final Configuration otherConf;
    public SyncDatabase(
            String starRocksDatabase,
            Map<String, String> mysqlConf,
            Map<String, String> sinkConf,
            Map<String, String> tableConf,
            Map<String, String> otherConf) {
        this.starRocksDatabase = starRocksDatabase;
        this.mysqlConf = Configuration.fromMap(mysqlConf);
        this.sinkConf = Configuration.fromMap(sinkConf);
        this.createTableConf = new CreateTableConf(Configuration.fromMap(tableConf));
        this.otherConf = Configuration.fromMap(otherConf);
    }

    public void build() throws Exception {
        TableIdMapping tableMapping = new TableIdMapping.SingleDataBaseSync(starRocksDatabase);
        tryCreateStarRocksTables(tableMapping);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MySqlSource<String> mySqlSource = buildSource();
        DataStream<String> source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .uid("mysql-source");
        StarRocksCatalog catalog = new StarRocksCatalog(
                sinkConf.get(StarRocksSinkOptions.JDBC_URL),
                sinkConf.get(StarRocksSinkOptions.USERNAME),
                sinkConf.get(StarRocksSinkOptions.PASSWORD));
        DataStream<StarRocksRowWithShuffleKey> rowStream = source.forward()
                .flatMap(new DebeziumJsonEventProcessor(catalog, tableMapping, createTableConf))
                .uid("event-parser");

        SinkFunction<StarRocksRowData> sinkFunction = buildSinkFunction();
        DataStreamSink<StarRocksRowData> sink = rowStream.keyBy(key -> key)
                .map(StarRocksRowWithShuffleKey::getRowData)
                .addSink(sinkFunction);
        if (sinkConf.contains(StarRocksSinkOptions.SINK_PARALLELISM)) {
            sink.setParallelism(sinkConf.get(StarRocksSinkOptions.SINK_PARALLELISM));
        }

        String pipelineName = env.getConfiguration().getOptional(PipelineOptions.NAME)
                .orElse(String.format("sync-mysql-to-sr-%s", starRocksDatabase));
        env.execute(pipelineName);
    }

    private void tryCreateStarRocksTables(TableIdMapping tableIdMapping) throws Exception {
        String jdbcUrl = JdbcUtils.getJdbcUrl(
                mysqlConf.get(MySqlSourceOptions.HOSTNAME),
                mysqlConf.get(MySqlSourceOptions.PORT)
            );
        Predicate<String> dbPredicate = RegexUtils.createDbPredicate(
                mysqlConf.get(MySqlSourceOptions.DATABASE_NAME));
        Predicate<String> tablePredicate = RegexUtils.createTablePredicate(
                otherConf.get(SyncOptions.INCLUDING_TABLES),
                otherConf.get(SyncOptions.EXCLUDING_TABLES)
            );
        List<Table> mysqlTables = MysqlUtils.getTables(
                jdbcUrl,
                mysqlConf.get(MySqlSourceOptions.USERNAME),
                mysqlConf.get(MySqlSourceOptions.PASSWORD),
                dbPredicate,
                tablePredicate
            );

        List<StarRocksTable> starRocksTables = new ArrayList<>();
        for (Table mysqlTable : mysqlTables) {
            TableId starRocksTableId = tableIdMapping.map(mysqlTable.getTableId());
            Schema starRocksSchema = MysqlUtils.toStarRocksSchema(mysqlTable.getSchema());
            StarRocksTable starRocksTable = new StarRocksTable(
                    starRocksTableId.getDatabaseName(),
                    starRocksTableId.getTableName(),
                    starRocksSchema);
            CreateTableConf.setStarRocksTable(starRocksTable, createTableConf);
            starRocksTables.add(starRocksTable);
        }

        StarRocksCatalog catalog = new StarRocksCatalog(
                sinkConf.get(StarRocksSinkOptions.JDBC_URL),
                sinkConf.get(StarRocksSinkOptions.USERNAME),
                sinkConf.get(StarRocksSinkOptions.PASSWORD));
        try {
            catalog.open();
            catalog.createDataBase(starRocksDatabase, true);
            for (StarRocksTable table : starRocksTables) {
                catalog.createTable(table, true);
            }
        } finally {
            catalog.close();
        }
    }

    private SinkFunction<StarRocksRowData> buildSinkFunction() {
        StarRocksSinkOptions.Builder builder = StarRocksSinkOptions.builder();
        for (Map.Entry<String, String> entry : sinkConf.toMap().entrySet()) {
            builder.withProperty(entry.getKey(), entry.getValue());
        }
        // force to use json format for schema compatibility after schema change
        StarRocksSinkOptions sinkOptions = builder
                        .withProperty(StarRocksSinkOptions.DATABASE_NAME.key(), ".*")
                        .withProperty(StarRocksSinkOptions.TABLE_NAME.key(), ".*")
                        .withProperty("sink.properties.format", "json")
                        .withProperty("sink.properties.strip_outer_array", "true")
                        .build();
        return SinkFunctionFactory.createSinkFunction(sinkOptions);
    }

    private MySqlSource<String> buildSource() {
        MySqlSourceBuilder<String> sourceBuilder = MySqlSource.builder();
        // Configure the source according to https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options
        // Note that MysqlSourceOptions.TABLE_NAME is ignored, and use SyncOptions.INCLUDING_TABLES and SyncOptions.EXCLUDING_TABLES instead
        String dbRegex = mysqlConf.get(MySqlSourceOptions.DATABASE_NAME);
        String includingTableRegex = otherConf.get(SyncOptions.INCLUDING_TABLES);
        String excludingTableRegex = otherConf.get(SyncOptions.EXCLUDING_TABLES);
        sourceBuilder.hostname(mysqlConf.get(MySqlSourceOptions.HOSTNAME))
            .port(mysqlConf.get(MySqlSourceOptions.PORT))
            .username(mysqlConf.get(MySqlSourceOptions.USERNAME))
            .password(mysqlConf.get(MySqlSourceOptions.PASSWORD))
            .databaseList(dbRegex)
            .tableList(RegexUtils.buildTableListRegex(dbRegex, includingTableRegex, excludingTableRegex));

        // options for connection
        mysqlConf.getOptional(MySqlSourceOptions.SERVER_ID)
                .ifPresent(sourceBuilder::serverId);
        mysqlConf.getOptional(MySqlSourceOptions.SERVER_TIME_ZONE)
                .ifPresent(sourceBuilder::serverTimeZone);
        mysqlConf.getOptional(MySqlSourceOptions.CONNECT_TIMEOUT)
                .ifPresent(sourceBuilder::connectTimeout);
        mysqlConf.getOptional(MySqlSourceOptions.CONNECT_MAX_RETRIES)
                .ifPresent(sourceBuilder::connectMaxRetries);
        mysqlConf.getOptional(MySqlSourceOptions.CONNECTION_POOL_SIZE)
                .ifPresent(sourceBuilder::connectionPoolSize);
        mysqlConf.getOptional(MySqlSourceOptions.HEARTBEAT_INTERVAL)
                .ifPresent(sourceBuilder::heartbeatInterval);

        // options for scan
        mysqlConf.getOptional(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE)
                .ifPresent(sourceBuilder::splitSize);
        sourceBuilder.startupOptions(getStartupOptions(mysqlConf));

        // options for jdbc
        Map<String, String> jdbcOptions = getPrefixConfigs(JdbcUrlUtils.PROPERTIES_PREFIX, mysqlConf);
        Properties jdbcProperties = new Properties();
        jdbcProperties.putAll(jdbcOptions);
        sourceBuilder.jdbcProperties(jdbcProperties);

        // options for debezium
        Map<String, String> debeziumOptions = getPrefixConfigs(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX, mysqlConf);
        Properties debeziumProperties = new Properties();
        debeziumProperties.putAll(debeziumOptions);
        sourceBuilder.debeziumProperties(debeziumProperties);

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(true, customConverterConfigs);

        boolean scanNewlyAddedTables = mysqlConf.get(MySqlSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED);
        return sourceBuilder
                .deserializer(schema)
                .includeSchemaChanges(true)
                .scanNewlyAddedTableEnabled(scanNewlyAddedTables)
                .build();
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET = "specific-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    // Refer to https://github.com/ververica/flink-cdc-connectors/blob/master/flink-connector-mysql-cdc/src/main/java/com/ververica/cdc/connectors/mysql/table/MySqlTableSourceFactory.java#L217C8-L217C8
    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SCAN_STARTUP_MODE);
        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            case SCAN_STARTUP_MODE_VALUE_EARLIEST:
                return StartupOptions.earliest();

            case SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET:
                return getSpecificOffset(config);

            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                return StartupOptions.timestamp(config.get(SCAN_STARTUP_TIMESTAMP_MILLIS));

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_EARLIEST,
                                SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }

    private static StartupOptions getSpecificOffset(ReadableConfig config) {
        BinlogOffsetBuilder offsetBuilder = BinlogOffset.builder();

        // GTID set
        config.getOptional(SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET)
                .ifPresent(offsetBuilder::setGtidSet);

        // Binlog file + pos
        Optional<String> binlogFilename = config.getOptional(SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
        Optional<Long> binlogPosition = config.getOptional(SCAN_STARTUP_SPECIFIC_OFFSET_POS);
        if (binlogFilename.isPresent() && binlogPosition.isPresent()) {
            offsetBuilder.setBinlogFilePosition(binlogFilename.get(), binlogPosition.get());
        } else {
            offsetBuilder.setBinlogFilePosition("", 0);
        }

        config.getOptional(SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS)
                .ifPresent(offsetBuilder::setSkipEvents);
        config.getOptional(SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS)
                .ifPresent(offsetBuilder::setSkipRows);
        return StartupOptions.specificOffset(offsetBuilder.build());
    }
}

