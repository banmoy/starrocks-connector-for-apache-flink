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

import com.esotericsoftware.minlog.Log;
import com.starrocks.connector.flink.container.StarRocksCluster;
import org.apache.flink.types.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Abstract IT case class for StarRocks. */
public abstract class StarRocksITTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksITTestBase.class);

    public static StarRocksCluster STARROCKS_CLUSTER =
            new StarRocksCluster(1, 0, 3);

    private static Connection DB_CONNECTION;

    @BeforeClass
    public static void setUp() throws Exception {
        try {
            STARROCKS_CLUSTER.start();
            LOG.info("StarRocks cluster is started.");
        } catch (Exception e) {
            LOG.error("Failed to star StarRocks cluster.", e);
            throw e;
        }

        try {
            DB_CONNECTION = DriverManager.getConnection(getJdbcUrl());
            LOG.info("Success to create db connection via jdbc {}", getJdbcUrl());
        } catch (Exception e) {
            LOG.error("Failed to create db connection via jdbc {}", getJdbcUrl(), e);
            throw e;
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (DB_CONNECTION != null) {
            DB_CONNECTION.close();
            LOG.info("Close db connection");
        }

        if (STARROCKS_CLUSTER != null) {
            STARROCKS_CLUSTER.stop();
            STARROCKS_CLUSTER.close();
            Log.info("Stop and close StarRocks cluster.");
        }
    }

    protected static String getJdbcUrl() {
        return "jdbc:mysql://" + STARROCKS_CLUSTER.getQueryUrls();
    }

    /**
     * Query the data in the table, and compare the result with the expected data.
     *
     * <p> For example, we can call this method as follows:
     *      verifyResult(
     *                 new Row[] {
     *                     Row.of("1", 1.4, Timestamp.valueOf("2022-09-06 20:00:00.000")),
     *                     Row.of("2", 2.4, Timestamp.valueOf("2022-09-06 20:00:00.000")),
     *                 },
     *                 "test_db",
     *                 "test_table
     *             );
     */
    public static void verifyResult(Row[] expectedData, String dbName, String tableName) throws SQLException {
        try (Connection dbConn = DriverManager.getConnection(getJdbcUrl());
                PreparedStatement statement = dbConn.prepareStatement(
                        String.format("select * from %s.%s", dbName, tableName));
                ResultSet resultSet = statement.executeQuery()) {
            int rowCount = 0;
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                Row row = new Row(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    row.setField(i, resultSet.getObject(i));
                }
                rowCount++;
                assertTrue(rowCount < expectedData.length);
                Row expectedRow = expectedData[rowCount - 1];
                assertEquals(expectedRow.getArity(), columnCount);
                assertEquals(expectedRow.toString(), row.toString());
            }
            assertEquals(rowCount, expectedData.length);
        }
    }
}
