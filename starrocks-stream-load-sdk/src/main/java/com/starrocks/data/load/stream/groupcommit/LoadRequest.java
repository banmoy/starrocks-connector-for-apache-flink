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

package com.starrocks.data.load.stream.groupcommit;

import com.starrocks.data.load.stream.Chunk;
import com.starrocks.data.load.stream.StreamLoadResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadRequest {

    private final GroupCommitTable table;
    private final Chunk chunk;
    private final AtomicInteger numRetries;
    private final List<Throwable> throwables;
    private String label;
    private StreamLoadResponse response;
    private CompletableFuture<?> future;

    public final long createTimeMs;
    // GroupCommitStreamLoader#sendBrpc
    public long executeTimeMs;
    public long compressTimeMs;
    public long callRpcTimeMs;
    public long receiveResponseTimeMs;
    public long labelFinalTimeMs;

    public long rawSize;
    public long compressSize;
    PGroupCommitLoadResponse loadResponse;

    public LoadRequest(GroupCommitTable table, Chunk chunk) {
        this.table = table;
        this.chunk = chunk;
        this.numRetries = new AtomicInteger(0);
        this.throwables = new ArrayList<>();
        this.createTimeMs = System.currentTimeMillis();
    }

    public void incRetries() {
        numRetries.getAndIncrement();
    }

    public int getRetries() {
        return numRetries.get();
    }

    public GroupCommitTable getTable() {
        return table;
    }

    public Chunk getChunk() {
        return chunk;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    public void setResponse(StreamLoadResponse response) {
        this.response = response;
    }

    public StreamLoadResponse getResponse() {
        return response;
    }

    public void addThrowable(Throwable throwable) {
        this.throwables.add(throwable);
    }

    public Throwable getFirstThrowable() {
        return throwables.isEmpty() ? null : throwables.get(0);
    }

    public Throwable getLastThrowable() {
        return throwables.isEmpty() ? null : throwables.get(throwables.size() - 1);
    }

    public List<Throwable> getThrowables() {
        return throwables;
    }

    public void setFuture(CompletableFuture<?> future) {
        this.future = future;
    }

    public CompletableFuture<?> getFuture() {
        return future;
    }

    public void reset() {
        label = null;
        response = null;
        future = null;
    }
}
