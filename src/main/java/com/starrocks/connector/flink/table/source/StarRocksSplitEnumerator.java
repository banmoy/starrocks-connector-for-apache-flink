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

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/** {@link SplitEnumerator} for starrocks source. */
public class StarRocksSplitEnumerator implements SplitEnumerator<StarRocksSplit, StarRocksEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSplitEnumerator.class);

    private final SplitEnumeratorContext<StarRocksSplit> context;

    public StarRocksSplitEnumerator(SplitEnumeratorContext<StarRocksSplit> context) {
        this.context = context;
    }

    @Override
    public void start() {
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    }

    @Override
    public void addSplitsBack(List<StarRocksSplit> splits, int subtaskId) {
    }

    @Override
    public void addReader(int subtaskId) {
        context.sendEventToSourceReader(subtaskId, new StarRocksSourceEvent(context.currentParallelism()));
    }

    @Override
    public StarRocksEnumState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {
    }
}
