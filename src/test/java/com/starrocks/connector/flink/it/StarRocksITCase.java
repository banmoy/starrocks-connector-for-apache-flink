/*
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

package com.starrocks.connector.flink.it;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkSemantic;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/** IT tests for StarRocks sink and source. */
public class StarRocksITCase extends StarRocksITTestBase {

    private final static String DB_NAME = "starrocks_connector_it";


    @BeforeClass
    public static void prepare() {
        String createDbCmd = "CREATE DATABASE " + DB_NAME;
        STARROCKS_CLUSTER.executeMysqlCommand(createDbCmd);
    }

    @Test
    public void testSink() {
        StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("load-url", STARROCKS_CLUSTER.getHttpUrls())
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", "sink_table")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("sink.label-prefix", "sink-prefix")
                .withProperty("sink.semantic", StarRocksSinkSemantic.AT_LEAST_ONCE.getName())
                .withProperty("sink.buffer-flush.interval-ms", "2000")
                .withProperty("sink.buffer-flush.max-bytes", "74002019")
                .withProperty("sink.buffer-flush.max-rows", "1002000")
                .withProperty("sink.max-retries", "2")
                .withProperty("sink.connect.timeout-ms", "2000")
                .build();

        String createTable =
                "CREATE TABLE " + String.join(".", DB_NAME, sinkOptions.getTableName()) + " (" +
                    "name STRING," +
                    "score BIGINT," +
                    "a DATETIME," +
                    "b STRING," +
                    "c DECIMAL(2,1)," +
                    "d DATE" +
                 ") ENGINE = OLAP " +
                 "DUPLICATE KEY(name)" +
                 "DISTRIBUTED BY HASH (name) BUCKETS 8";
        STARROCKS_CLUSTER.executeMysqlCommand(createTable);

        Configuration conf = new Configuration();
        conf.setBoolean("classloader.check-leaked-classloader", false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        String createSQL = "CREATE TABLE USER_RESULT(" +
                "name STRING," +
                "score BIGINT," +
                "a TIMESTAMP(3)," +
                "b STRING," +
                "c DECIMAL(2,1)," +
                "d DATE" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + sinkOptions.getJdbcUrl() + "'," +
                "'load-url'='" + String.join(";", sinkOptions.getLoadUrlList()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + sinkOptions.getTableName() + "'," +
                "'username' = '" + sinkOptions.getUsername() + "'," +
                "'password' = '" + sinkOptions.getPassword() + "'," +
                "'sink.buffer-flush.max-rows' = '" + sinkOptions.getSinkMaxRows() + "'," +
                "'sink.buffer-flush.max-bytes' = '" + sinkOptions.getSinkMaxBytes() + "'," +
                "'sink.buffer-flush.interval-ms' = '" + sinkOptions.getSinkMaxFlushInterval() + "'," +
                "'sink.buffer-flush.enqueue-timeout-ms' = '" + sinkOptions.getSinkOfferTimeout() + "'," +
                "'sink.properties.column_separator' = '\\x01'," +
                "'sink.properties.row_delimiter' = '\\x02'" +
                ")";
        tEnv.executeSql(createSQL);

        String exMsg = "";
        try {
            tEnv.executeSql("INSERT INTO USER_RESULT\n" +
                    "VALUES ('lebron', 99, TO_TIMESTAMP('2020-01-01 01:00:01'), 'b', 2.3, TO_DATE('2020-01-01'))").collect();
            Thread.sleep(2000);
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertFalse(exMsg, exMsg.length() > 0);
    }

    @Test
    public void testSource() {
        StarRocksSourceOptions options = StarRocksSourceOptions.builder()
                .withProperty("scan-url", STARROCKS_CLUSTER.getHttpUrls())
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", "source_table")
                .build();

        String createTable =
                "CREATE TABLE " + String.join(".", DB_NAME, options.getTableName()) + " (" +
                            "date_1 DATE," +
                            "datetime_1 DATETIME,"+
                            "char_1 CHAR(3),"+
                            "varchar_1 VARCHAR(10),"+
                            "boolean_1 BOOLEAN,"+
                            "tinyint_1 TINYINT,"+
                            "smallint_1 SMALLINT,"+
                            "int_1 INT,"+
                            "bigint_1 BIGINT,"+
                            "float_1 FLOAT,"+
                            "double_1 DOUBLE"+
                        ") ENGINE = OLAP " +
                        "DUPLICATE KEY(date_1)" +
                        "DISTRIBUTED BY HASH (date_1) BUCKETS 8";
        STARROCKS_CLUSTER.executeMysqlCommand(createTable);

        String inserIntoData =
                "INSERT INTO " + String.join(".", DB_NAME, options.getTableName()) + " " +
                        "WITH LABEL starrocks_source_test VALUES" +
                        "('2022-09-01', '2022-09-01 21:28:30', 'c11', 'vc11', 0, 125, -32000, 600000, 9232322, 1.2, 2.23)," +
                        "('2022-09-02', '2022-09-02 05:12:32', 'c12', 'vc12', 1, 3, 948, 93, -12, 43.334, -9494.2);";
        STARROCKS_CLUSTER.executeMysqlCommand(inserIntoData);

        Configuration conf = new Configuration();
        conf.setBoolean("classloader.check-leaked-classloader", false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE flink_type_test (" +
                            "date_1 DATE," +
                            "datetime_1 TIMESTAMP(6),"+
                            "char_1 CHAR(3),"+
                            "varchar_1 VARCHAR(10),"+
                            "boolean_1 BOOLEAN,"+
                            "tinyint_1 TINYINT,"+
                            "smallint_1 SMALLINT,"+
                            "int_1 INT,"+
                            "bigint_1 BIGINT,"+
                            "float_1 FLOAT,"+
                            "double_1 DOUBLE"+
                        ") WITH (\n" +
                        "  'connector' = 'starrocks',\n" +
                        "  'scan-url' = '" + options.getScanUrl() + "',\n" +
                        "  'scan.connect.timeout-ms' = '5000', " +
                        "  'jdbc-url' = '" + options.getJdbcUrl() + "',\n" +
                        "  'username' = '" + options.getUsername() + "',\n" +
                        "  'password' = '" + options.getPassword() + "',\n" +
                        "  'database-name' = '" + options.getDatabaseName() + "',\n" +
                        "  'table-name' = '" + options.getTableName() + "')"
        );
        Exception e = null;
        try {
            tEnv.executeSql("select * from flink_type_test").print();
            Thread.sleep(2000);
        } catch (Exception ex) {
            ex.printStackTrace();
            e = ex;
        }
        assertNull(e);
    }


    @Test
    public void testLookup() {
        StarRocksSourceOptions options = StarRocksSourceOptions.builder()
                .withProperty("scan-url", STARROCKS_CLUSTER.getHttpUrls())
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", "lookup_table")
                .build();

        String createTable =
                "CREATE TABLE " + String.join(".", DB_NAME, options.getTableName()) + " (" +
                        "date_1 DATE," +
                        "datetime_1 DATETIME,"+
                        "char_1 CHAR(3),"+
                        "varchar_1 VARCHAR(10),"+
                        "boolean_1 BOOLEAN,"+
                        "tinyint_1 TINYINT,"+
                        "smallint_1 SMALLINT,"+
                        "int_1 INT,"+
                        "bigint_1 BIGINT,"+
                        "float_1 FLOAT,"+
                        "double_1 DOUBLE"+
                        ") ENGINE = OLAP " +
                        "DUPLICATE KEY(date_1)" +
                        "DISTRIBUTED BY HASH (date_1) BUCKETS 8";
        STARROCKS_CLUSTER.executeMysqlCommand(createTable);

        String inserIntoData =
                "INSERT INTO " + String.join(".", DB_NAME, options.getTableName()) + " " +
                        "WITH LABEL starrocks_lookup_test VALUES" +
                        "('2022-09-01', '2022-09-01 21:28:30', 'c11', 'vc11', 0, 125, -32000, 600000, 9232322, 1.2, 2.23)," +
                        "('2022-09-02', '2022-09-02 05:12:32', 'c12', 'vc12', 1, 3, 948, 93, -12, 43.334, -9494.2);";
        STARROCKS_CLUSTER.executeMysqlCommand(inserIntoData);

        Configuration conf = new Configuration();
        conf.setBoolean("classloader.check-leaked-classloader", false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        Exception e = null;
        try {
            RowTypeInfo testTypeInfo = new RowTypeInfo(
                            new TypeInformation[] {Types.BOOLEAN, Types.STRING},
                            new String[] {"k1", "k2"});
            List<Row> testData = new ArrayList<>();
            testData.add(Row.of(false, "row1"));
            testData.add(Row.of(true, "row2"));

            DataStream<Row> srcDs = env.fromCollection(testData).returns(testTypeInfo);
            Table in = tEnv.fromDataStream(srcDs, $("k1"), $("k2"), $("proctime").proctime());
            tEnv.registerTable("datagen", in);

            tEnv.executeSql(
                    "CREATE TABLE flink_type_test (" +
                                "date_1 DATE," +
                                "datetime_1 TIMESTAMP(6),"+
                                "char_1 CHAR(3),"+
                                "varchar_1 VARCHAR(10),"+
                                "boolean_1 BOOLEAN,"+
                                "tinyint_1 TINYINT,"+
                                "smallint_1 SMALLINT,"+
                                "int_1 INT,"+
                                "bigint_1 BIGINT,"+
                                "float_1 FLOAT,"+
                                "double_1 DOUBLE"+
                            ") WITH (\n" +
                            "'connector' = 'starrocks',\n" +
                            "'scan-url' = '" + options.getScanUrl() + "',\n" +
                            "'scan.connect.timeout-ms' = '5000', " +
                            "'jdbc-url' = '" + options.getJdbcUrl() + "',\n" +
                            "'username' = '" + options.getUsername() + "',\n" +
                            "'password' = '" + options.getPassword() + "',\n" +
                            "'database-name' = '" + options.getDatabaseName() + "',\n" +
                            "'table-name' = '" + options.getTableName() + "')"
            );

            tEnv.executeSql("SELECT * FROM datagen LEFT JOIN flink_type_test FOR SYSTEM_TIME AS OF datagen.proctime ON " +
                "datagen.k1 = flink_type_test.boolean_1").print();

            Thread.sleep(2000);
        } catch (Exception ex) {
            ex.printStackTrace();
            e = ex;
        }
        assertNull(e);
    }
}
