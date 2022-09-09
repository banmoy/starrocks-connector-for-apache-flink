package com.starrocks.connector.flink.it.source;

import com.starrocks.connector.flink.StarRocksSource;
import com.starrocks.connector.flink.table.source.StarRocksSourceCommonFunc;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import mockit.Mock;
import mockit.MockUp;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class StarRocksDynamicTableSourceITTest extends StarRocksSourceBaseTest {

    @Parameterized.Parameters(name = "Use New Api = {0}")
    public static List<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Parameterized.Parameter
    public boolean useNewApi;

    private Long dataCount = 30L;

    @Test
    public void testDataStream() throws Exception {
        new MockUp<StarRocksSourceCommonFunc>() {
            @Mock
            public Long getQueryCount(StarRocksSourceOptions sourceOptions, String SQL) {
                return dataCount;
            }
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<RowData> dList;
        if (useNewApi) {
            dList = env.fromSource(
                        StarRocksSource.newSource(TABLE_SCHEMA, OPTIONS_WITH_COLUMN_IS_COUNT),
                            WatermarkStrategy.noWatermarks(), "Test Source")
                    .setParallelism(5).executeAndCollect(50);
        } else {
             dList = env.addSource(StarRocksSource.source(TABLE_SCHEMA, OPTIONS_WITH_COLUMN_IS_COUNT))
                    .setParallelism(5).executeAndCollect(50);
        }

        assertEquals(dList.size(), (long) dataCount);
    }

    @Test
    public void testTableAPI() {
        mockOneBeResonsefunc();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE flink_type_test (" +
                    "date_1 DATE," +
                    "datetime_1 TIMESTAMP(6),"+
                    "char_1 CHAR(20),"+
                    "varchar_1 VARCHAR,"+
                    "boolean_1 BOOLEAN,"+
                    "tinyint_1 TINYINT,"+
                    "smallint_1 SMALLINT,"+
                    "int_1 INT,"+
                    "bigint_1 BIGINT,"+
                    "largeint_1 STRING,"+
                    "float_1 FLOAT,"+
                    "double_1 DOUBLE,"+
                    "decimal_1 DECIMAL(10,9)\n"+
                ") WITH (\n" +
                    "  'connector' = 'starrocks',\n" +
                    "  'scan-url' = '" + OPTIONS.getScanUrl() + "',\n" +
                    "  'scan.connect.timeout-ms' = '5000', " +
                    "  'jdbc-url' = '" + OPTIONS.getJdbcUrl() + "',\n" +
                    "  'username' = '" + OPTIONS.getUsername() + "',\n" +
                    "  'password' = '" + OPTIONS.getPassword() + "',\n" +
                    "  'database-name' = '" + OPTIONS.getDatabaseName() + "',\n" +
                    "  'table-name' = '" + OPTIONS.getTableName() + "',\n" +
                    "  'scan.use.new-api' = '" + useNewApi + "'" +
                    ")"
                );
        Exception e = null;
        try {
            tEnv.executeSql("select * from flink_type_test").print();;
            Thread.sleep(5000);
        } catch (Exception ex) {
            ex.printStackTrace();
            e = ex;
        }
        assertTrue(e == null);
    }

    @Test
    public void testLookupTableAPI() {
        Exception e = null;
        try {            
            Thread thriftThread = new Thread(() -> {
                mockOneBeResonsefunc();
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                StreamTableEnvironment tEnv;
                tEnv = StreamTableEnvironment.create(env);
                tEnv.executeSql(
                    "CREATE TABLE datagen (" +
                        "k1 BOOLEAN,"+
                        "k2 SMALLINT,"+
                        "proctime as PROCTIME() " +
                    ") WITH ( " +
                        "'connector' = 'datagen', " +
                        "'rows-per-second'='1'" +
                    ")"
                );
                tEnv.executeSql(
                    "CREATE TABLE flink_type_test (" +
                        "date_1 DATE," +
                        "datetime_1 TIMESTAMP(6),"+
                        "char_1 CHAR(20),"+
                        "varchar_1 VARCHAR,"+
                        "boolean_1 BOOLEAN,"+
                        "tinyint_1 TINYINT,"+
                        "smallint_1 SMALLINT,"+
                        "int_1 INT,"+
                        "bigint_1 BIGINT,"+
                        "largeint_1 STRING,"+
                        "float_1 FLOAT,"+
                        "double_1 DOUBLE,"+
                        "decimal_1 DECIMAL(10,9)\n"+
                    ") WITH (\n" +
                        "'connector' = 'starrocks',\n" +
                        "'scan-url' = '" + OPTIONS.getScanUrl() + "',\n" +
                        "'scan.connect.timeout-ms' = '5000', " +
                        "'jdbc-url' = '" + OPTIONS.getJdbcUrl() + "',\n" +
                        "'username' = '" + OPTIONS.getUsername() + "',\n" +
                        "'password' = '" + OPTIONS.getPassword() + "',\n" +
                        "'database-name' = '" + OPTIONS.getDatabaseName() + "',\n" +
                        "'table-name' = '" + OPTIONS.getTableName() + "')"
                );
                tEnv.executeSql("SELECT * FROM datagen LEFT JOIN flink_type_test FOR SYSTEM_TIME AS OF datagen.proctime ON " + 
                                "datagen.k1 = flink_type_test.boolean_1").print();
            });
            thriftThread.start();
            Thread.sleep(5000);
            throw new RuntimeException("Time is up");
        } catch (Exception ex) {
            e = ex;
        }
        assertTrue(e.getMessage().equals("Time is up"));
    }

    @Test
    public void testSourceCommonProperties() {
        assertEquals(JDBC_URL, OPTIONS.getJdbcUrl());
        assertEquals(SCAN_URL, OPTIONS.getScanUrl());
        assertEquals(DATABASE, OPTIONS.getDatabaseName());
        assertEquals(TABLE, OPTIONS.getTableName());
        assertEquals(USERNAME, OPTIONS.getUsername());
        assertEquals(PASSWORD, OPTIONS.getPassword());
    }

    private boolean checkCause(Throwable throwable, String causeStr) {
        if (null == throwable) {
            return false;
        }
        if (throwable.getMessage().contains(causeStr)) {
            return true;
        }
        return checkCause(throwable.getCause(), causeStr);
    }
}
