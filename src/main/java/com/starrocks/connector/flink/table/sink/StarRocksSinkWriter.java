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

package com.starrocks.connector.flink.table.sink;

import com.google.common.base.Strings;
import com.starrocks.connector.flink.manager.StarRocksSinkBufferEntity;
import com.starrocks.connector.flink.manager.StarRocksSinkManager;
import com.starrocks.connector.flink.row.sink.StarRocksIRowTransformer;
import com.starrocks.connector.flink.row.sink.StarRocksISerializer;
import com.starrocks.connector.flink.tools.StarRocksException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.truncate.Truncate;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.NestedRowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Implementation of {@link StatefulSink.StatefulSinkWriter} for StarRocks connector. */
public class StarRocksSinkWriter<T> implements StatefulSink.StatefulSinkWriter<T, Map<String, StarRocksSinkBufferEntity>> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkWriter.class);

    private final StarRocksSinkManager sinkManager;
    private final StarRocksIRowTransformer<T> rowTransformer;
    private final StarRocksSinkOptions sinkOptions;
    private final StarRocksISerializer serializer;

    private final Counter totalInvokeRowsTime;
    private final Counter totalInvokeRows;
    private static final String COUNTER_INVOKE_ROWS_COST_TIME = "totalInvokeRowsTimeNs";
    private static final String COUNTER_INVOKE_ROWS = "totalInvokeRows";

    private Map<String, StarRocksSinkBufferEntity> previousState;

    public StarRocksSinkWriter(
            StarRocksSinkManager sinkManager,
            StarRocksIRowTransformer<T> rowTransformer,
            StarRocksSinkOptions sinkOptions,
            StarRocksISerializer serializer,
            Collection<Map<String, StarRocksSinkBufferEntity>> recoveredState,
            Sink.InitContext sinkContext) throws IOException {
        this.sinkManager = sinkManager;
        this.rowTransformer = rowTransformer;
        this.sinkOptions = sinkOptions;
        this.serializer = serializer;
        this.previousState = recoveredState.isEmpty() ? Collections.emptyMap() : recoveredState.iterator().next();

        sinkManager.setMetrics(sinkContext.metricGroup());
        totalInvokeRows = sinkContext.metricGroup().counter(COUNTER_INVOKE_ROWS);
        totalInvokeRowsTime = sinkContext.metricGroup().counter(COUNTER_INVOKE_ROWS_COST_TIME);
        if (null != rowTransformer) {
            rowTransformer.setRuntimeContext(null);
        }
        sinkManager.startScheduler();
        sinkManager.startAsyncFlushing();
    }

    @Override
    public void write(T value, Context context) throws IOException, InterruptedException {
        long start = System.nanoTime();
        if (StarRocksSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            flushPreviousState();
        }
        if (null == serializer) {
            if (value instanceof StarRocksSinkRowDataWithMeta) {
                StarRocksSinkRowDataWithMeta data = (StarRocksSinkRowDataWithMeta)value;
                if (Strings.isNullOrEmpty(data.getDatabase()) || Strings.isNullOrEmpty(data.getTable()) || null == data.getDataRows()) {
                    LOG.warn(String.format("json row data not fullfilled. {database: %s, table: %s, dataRows: %s}", data.getDatabase(), data.getTable(), data.getDataRows()));
                    return;
                }
                sinkManager.writeRecords(data.getDatabase(), data.getTable(), data.getDataRows());
                return;
            }
            // raw data sink
            sinkManager.writeRecords(sinkOptions.getDatabaseName(), sinkOptions.getTableName(), (String) value);
            totalInvokeRows.inc(1);
            totalInvokeRowsTime.inc(System.nanoTime() - start);
            return;
        }
        if (value instanceof NestedRowData) {
            final int headerSize = 256;
            NestedRowData ddlData = (NestedRowData) value;
            if (ddlData.getSegments().length != 1 || ddlData.getSegments()[0].size() < headerSize) {
                return;
            }
            int totalSize = ddlData.getSegments()[0].size();
            byte[] data = new byte[totalSize - headerSize];
            ddlData.getSegments()[0].get(headerSize, data);
            Map<String, String> ddlMap;
            try {
                ddlMap = InstantiationUtil.deserializeObject(data, HashMap.class.getClassLoader());
            } catch (Exception e) {
                throw new StarRocksException(e);
            }
            if (null == ddlMap
                    || "true".equals(ddlMap.get("snapshot"))
                    || Strings.isNullOrEmpty(ddlMap.get("ddl"))
                    || Strings.isNullOrEmpty(ddlMap.get("databaseName"))) {
                return;
            }
            Statement stmt;
            try {
                stmt = CCJSqlParserUtil.parse(ddlMap.get("ddl"));
            } catch (Exception e) {
                throw new StarRocksException(e);
            }
            if (stmt instanceof Truncate) {
                Truncate truncate = (Truncate) stmt;
                if (!sinkOptions.getTableName().equalsIgnoreCase(truncate.getTable().getName())) {
                    return;
                }
                // TODO: add ddl to queue
            } else if (stmt instanceof Alter) {
                Alter alter = (Alter) stmt;
            }
        }
        if (value instanceof RowData) {
            if (RowKind.UPDATE_BEFORE.equals(((RowData)value).getRowKind())) {
                // do not need update_before, cauz an update action happened on the primary keys will be separated into `delete` and `create`
                return;
            }
            if (!sinkOptions.supportUpsertDelete() && RowKind.DELETE.equals(((RowData)value).getRowKind())) {
                // let go the UPDATE_AFTER and INSERT rows for tables who have a group of `unique` or `duplicate` keys.
                return;
            }
        }
        sinkManager.writeRecords(
                sinkOptions.getDatabaseName(),
                sinkOptions.getTableName(),
                serializer.serialize(rowTransformer.transform(value, sinkOptions.supportUpsertDelete()))
        );
        totalInvokeRows.inc(1);
        totalInvokeRowsTime.inc(System.nanoTime() - start);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (StarRocksSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            flushPreviousState();
        }
        try {
            sinkManager.flush(null, true);
        } catch (Exception e) {
            throw new StarRocksException(e);
        }
    }

    @Override
    public List<Map<String, StarRocksSinkBufferEntity>> snapshotState(long checkpointId) throws IOException {
        if (StarRocksSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            flushPreviousState();
            previousState = sinkManager.getBufferedBatchMap();
            return Collections.singletonList(previousState);
        }

        try {
            sinkManager.flush(null, true);
        } catch (Exception e) {
            throw new StarRocksException(e);
        }
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close sink writer");
        sinkManager.close();
    }

    private void flushPreviousState() throws IOException {
        // flush the batch saved at the previous checkpoint
        if (!previousState.isEmpty()) {
            sinkManager.setBufferedBatchMap(previousState);
            try {
                sinkManager.flush(null, true);
            } catch (Exception e) {
                throw new StarRocksException(e);
            }
            previousState = Collections.emptyMap();
        }
    }
}
