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

package com.starrocks.data.load.stream.v2;

import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.groupcommit.GroupCommitManager;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;

import java.io.Serializable;

public class StreamLoadManagerV2 implements StreamLoadManager, Serializable {

    private final StreamLoadManager manager;

    public StreamLoadManagerV2(StreamLoadProperties properties, boolean enableAutoCommit) {
        boolean enableGroupCommit = properties.getHeaders().containsKey("group_commit");
        this.manager = enableGroupCommit ? new GroupCommitManager(properties)
                : new StreamLoadManagerImpl(properties, enableAutoCommit);
    }

    @Override
    public void init() {
        manager.init();
    }

    @Override
    public void write(String uniqueKey, String database, String table, String... rows) {
        manager.write(uniqueKey, database, table, rows);
    }

    @Override
    public void callback(StreamLoadResponse response) {
        manager.callback(response);
    }

    @Override
    public void callback(Throwable e) {
        manager.callback(e);
    }

    @Override
    public void flush() {
        manager.flush();
    }

    @Override
    public StreamLoadSnapshot snapshot() {
        return manager.snapshot();
    }

    @Override
    public boolean prepare(StreamLoadSnapshot snapshot) {
        return manager.prepare(snapshot);
    }

    @Override
    public boolean commit(StreamLoadSnapshot snapshot) {
        return manager.commit(snapshot);
    }

    @Override
    public boolean abort(StreamLoadSnapshot snapshot) {
        return manager.abort(snapshot);
    }

    @Override
    public void close() {
        manager.close();
    }
}
