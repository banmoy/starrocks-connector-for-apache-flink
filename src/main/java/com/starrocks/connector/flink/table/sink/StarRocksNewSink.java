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

import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.manager.StarRocksQueryVisitor;
import com.starrocks.connector.flink.manager.StarRocksSinkBufferEntity;
import com.starrocks.connector.flink.manager.StarRocksSinkManager;
import com.starrocks.connector.flink.row.sink.StarRocksIRowTransformer;
import com.starrocks.connector.flink.row.sink.StarRocksISerializer;
import com.starrocks.connector.flink.row.sink.StarRocksSerializerFactory;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.api.TableSchema;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/** Implementation of {@link Sink} for StarRocks connector. */
public class StarRocksNewSink<T> implements StatefulSink<T, Map<String, StarRocksSinkBufferEntity>>, StatefulSink.WithCompatibleState {

    private static final long serialVersionUID = 1L;

    private final StarRocksSinkManager sinkManager;
    private final StarRocksIRowTransformer<T> rowTransformer;
    private final StarRocksSinkOptions sinkOptions;
    private final StarRocksISerializer serializer;

    public StarRocksNewSink(StarRocksSinkOptions sinkOptions, TableSchema schema, StarRocksIRowTransformer<T> rowTransformer) {
        StarRocksJdbcConnectionOptions
                jdbcOptions = new StarRocksJdbcConnectionOptions(sinkOptions.getJdbcUrl(), sinkOptions.getUsername(), sinkOptions.getPassword());
        StarRocksJdbcConnectionProvider jdbcConnProvider = new StarRocksJdbcConnectionProvider(jdbcOptions);
        StarRocksQueryVisitor starrocksQueryVisitor = new StarRocksQueryVisitor(jdbcConnProvider, sinkOptions.getDatabaseName(), sinkOptions.getTableName());
        this.sinkManager = new StarRocksSinkManager(sinkOptions, schema, jdbcConnProvider, starrocksQueryVisitor);

        rowTransformer.setStarRocksColumns(starrocksQueryVisitor.getFieldMapping());
        rowTransformer.setTableSchema(schema);
        this.serializer = StarRocksSerializerFactory.createSerializer(sinkOptions, schema.getFieldNames());
        this.rowTransformer = rowTransformer;
        this.sinkOptions = sinkOptions;
    }

    @Override
    public StatefulSinkWriter<T, Map<String, StarRocksSinkBufferEntity>> createWriter(InitContext context) throws IOException {
        return new StarRocksSinkWriter<>(
                sinkManager,
                rowTransformer,
                sinkOptions,
                serializer,
                Collections.emptyList(),
                context);
    }

    @Override
    public StatefulSinkWriter<T, Map<String, StarRocksSinkBufferEntity>> restoreWriter(
            InitContext context, Collection<Map<String, StarRocksSinkBufferEntity>> recoveredState)
            throws IOException {
        return new StarRocksSinkWriter<>(
                sinkManager,
                rowTransformer,
                sinkOptions,
                serializer,
                recoveredState,
                context);
    }

    @Override
    public SimpleVersionedSerializer<Map<String, StarRocksSinkBufferEntity>> getWriterStateSerializer() {
        return new BufferEntityMapSerializer();
    }

    @Override
    public Collection<String> getCompatibleWriterStateNames() {
        return Collections.singleton("buffered-rows");
    }
}
