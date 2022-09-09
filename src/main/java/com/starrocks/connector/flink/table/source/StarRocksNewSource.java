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

package com.starrocks.connector.flink.table.source;

import com.google.common.base.Strings;
import com.starrocks.connector.flink.table.source.struct.ColunmRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/** Implementation StarRocks source using {@link Source}. */
public class StarRocksNewSource implements Source<RowData, StarRocksSplit, StarRocksEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksNewSource.class);

    private final StarRocksSourceOptions sourceOptions;
    private QueryInfo queryInfo;
    private Long dataCount;
    private final SelectColumn[] selectColumns;
    private final List<ColunmRichInfo> colunmRichInfos;

    private StarRocksSourceQueryType queryType;

    public StarRocksNewSource(TableSchema flinkSchema, StarRocksSourceOptions sourceOptions) {
        this.sourceOptions = sourceOptions;
        Map<String, ColunmRichInfo> columnMap = StarRocksSourceCommonFunc.genColumnMap(flinkSchema);
        this.colunmRichInfos = StarRocksSourceCommonFunc.genColunmRichInfo(columnMap);
        String SQL = genSQL(sourceOptions);
        if (this.sourceOptions.getColumns().trim().toLowerCase().contains("count(")) {
            this.queryType = StarRocksSourceQueryType.QueryCount;
            this.dataCount = StarRocksSourceCommonFunc.getQueryCount(this.sourceOptions, SQL);
            this.selectColumns = null;
        } else {
            this.queryInfo = StarRocksSourceCommonFunc.getQueryInfo(this.sourceOptions, SQL);
            this.selectColumns = StarRocksSourceCommonFunc.genSelectedColumns(columnMap, sourceOptions, colunmRichInfos);
        }
    }

    public StarRocksNewSource(StarRocksSourceOptions sourceOptions, TableSchema flinkSchema,
                              String filter, long limit, SelectColumn[] selectColumns, String columns, StarRocksSourceQueryType queryType) {
        this.sourceOptions = sourceOptions;
        Map<String, ColunmRichInfo> columnMap = StarRocksSourceCommonFunc.genColumnMap(flinkSchema);
        this.colunmRichInfos = StarRocksSourceCommonFunc.genColunmRichInfo(columnMap);
        if (queryType == null) {
            queryType = StarRocksSourceQueryType.QueryAllColumns;
            this.selectColumns = StarRocksSourceCommonFunc.genSelectedColumns(columnMap, sourceOptions, colunmRichInfos);
        } else {
            this.selectColumns = selectColumns;
        }
        String SQL = genSQL(queryType, columns, filter, limit);
        if (queryType == StarRocksSourceQueryType.QueryCount) {
            this.dataCount = StarRocksSourceCommonFunc.getQueryCount(this.sourceOptions, SQL);
        } else {
            this.queryInfo = StarRocksSourceCommonFunc.getQueryInfo(this.sourceOptions, SQL);
        }
        this.queryType = queryType;
    }

    @Override
    public SourceReader<RowData, StarRocksSplit> createReader(SourceReaderContext readerContext) throws Exception {
        LOG.info("Create reader");
        return new StarRocksSourceReader(
                sourceOptions,
                queryInfo,
                dataCount,
                selectColumns,
                colunmRichInfos,
                queryType,
                readerContext);
    }

    @Override
    public SplitEnumerator<StarRocksSplit, StarRocksEnumState> createEnumerator(
            SplitEnumeratorContext<StarRocksSplit> enumContext) throws Exception {
        LOG.info("Create enumerator");
        return new StarRocksSplitEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<StarRocksSplit, StarRocksEnumState> restoreEnumerator(
            SplitEnumeratorContext<StarRocksSplit> enumContext, StarRocksEnumState checkpoint) throws Exception {
        LOG.info("Restore enumerator");
        return new StarRocksSplitEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<StarRocksSplit> getSplitSerializer() {
        return new StarRocksSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<StarRocksEnumState> getEnumeratorCheckpointSerializer() {
        return new StarRocksEnumStateSerializer();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    private String genSQL(StarRocksSourceOptions options) {
        String columns = options.getColumns().isEmpty() ? "*" : options.getColumns();
        String filter = options.getFilter().isEmpty() ? "" : " where " + options.getFilter();
        StringBuilder sqlSb = new StringBuilder("select ");
        sqlSb.append(columns);
        sqlSb.append(" from ");
        sqlSb.append("`" + sourceOptions.getDatabaseName() + "`");
        sqlSb.append(".");
        sqlSb.append("`" + sourceOptions.getTableName() + "`");
        sqlSb.append(filter);
        return sqlSb.toString();
    }

    private String genSQL(StarRocksSourceQueryType queryType, String columns, String filter, long limit) {
        StringBuilder sqlSb = new StringBuilder("select ");
        switch (queryType) {
            case QueryCount:
                sqlSb.append("count(*)");
                break;
            case QueryAllColumns:
                sqlSb.append("*");
                break;
            case QuerySomeColumns:
                sqlSb.append(columns);
                break;
        }
        sqlSb.append(" from ");
        sqlSb.append("`" + sourceOptions.getDatabaseName() + "`");
        sqlSb.append(".");
        sqlSb.append("`" + sourceOptions.getTableName() + "`");
        if (!Strings.isNullOrEmpty(filter)) {
            sqlSb.append(" where ");
            sqlSb.append(filter);
        }
        if (limit > 0) {
            // (not support) SQL = SQL + " limit " + limit;
            throw new RuntimeException("Read data from be not support limit now !");
        }
        return sqlSb.toString();
    }
}
