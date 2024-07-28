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
import com.starrocks.data.load.stream.compress.CompressionCodec;
import com.starrocks.data.load.stream.compress.CompressionHttpEntity;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.v2.ChunkHttpEntity;
import org.apache.http.HttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.starrocks.data.load.stream.exception.ErrorUtils.isRetryable;

public class GroupCommitTable {

    private static final Logger LOG = LoggerFactory.getLogger(GroupCommitTable.class);

    enum FlushChunkReason {

        CHUNK_FULL (false),
        CHUNK_DELAY (true),
        CACHE_EVICT(true),
        FLUSH (true);

        boolean force;

        FlushChunkReason(boolean force) {
            this.force = force;
        }

        boolean isForce() {
            return force;
        }
    }

    private final String database;
    private final String table;
    private final GroupCommitManager manager;
    protected final GroupCommitStreamLoader streamLoader;
    private final StreamLoadTableProperties properties;
    private final int maxRetries;
    private final int retryIntervalInMs;
    private final int flushIntervalMs;
    private final int flushTimeoutMs;
    private final long chunkSize;
    private final Optional<CompressionCodec> compressionCodec;
    private final AtomicLong chunkIdGenerator;
    private volatile Chunk activeChunk;
    private final Map<Long, LoadRequest> inflightLoadRequests;
    private final AtomicLong cacheBytes = new AtomicLong();
    private final AtomicLong cacheRows = new AtomicLong();
    private final ReentrantLock lock;
    private final Condition flushCondition;
    private volatile ScheduledFuture<?> timer;
    private final AtomicReference<Throwable> tableThrowable;

    public GroupCommitTable(
            String database,
            String table,
            GroupCommitManager manager,
            GroupCommitStreamLoader streamLoader,
            StreamLoadTableProperties properties,
            int maxRetries,
            int retryIntervalInMs,
            int flushIntervalMs,
            int flushTimeoutMs,
            long chunkSize) {
        this.database = database;
        this.table = table;
        this.manager = manager;
        this.streamLoader = streamLoader;
        this.properties = properties;
        this.maxRetries = maxRetries;
        this.retryIntervalInMs = retryIntervalInMs;
        this.flushIntervalMs = flushIntervalMs;
        this.flushTimeoutMs = flushTimeoutMs;
        this.chunkSize = chunkSize;
        this.compressionCodec = CompressionCodec.createCompressionCodec(
                properties.getDataFormat(),
                properties.getProperty("compression"),
                properties.getTableProperties());
        this.chunkIdGenerator = new AtomicLong();
        this.inflightLoadRequests = new ConcurrentHashMap<>();
        this.lock = new ReentrantLock();
        this.flushCondition = lock.newCondition();
        this.tableThrowable = new AtomicReference<>();
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public StreamLoadTableProperties getProperties() {
        return properties;
    }

    public int write(byte[] row) {
        lock.lock();
        try {
            Chunk chunk = getActiveChunk();
            chunk.addRow(row);
            cacheBytes.addAndGet(row.length);
            cacheRows.incrementAndGet();
            switchChunk(FlushChunkReason.CHUNK_FULL);
            return row.length;
        } finally {
            lock.unlock();
        }
    }

    public void flush() throws Exception {
        lock.lock();
        try {
            switchChunk(FlushChunkReason.FLUSH);
            long leftTimeoutNs = flushTimeoutMs * 1000000L;
            while (!inflightLoadRequests.isEmpty() && tableThrowable.get() == null) {
                try {
                    leftTimeoutNs = flushCondition.awaitNanos(leftTimeoutNs);
                    if (leftTimeoutNs <= 0) {
                        throw new RuntimeException(String.format(
                                "Timeout to wait flush, db: %s, table: %s, timeout: %s ms",
                                    database, table, flushTimeoutMs));
                    }
                } catch (Exception e) {
                    LOG.warn("Fail to wait flush, db: {}, table: {}", database, table, e);
                    throw e;
                }
            }
            if (tableThrowable.get() != null) {
                throw new RuntimeException(
                        String.format("Exception happened when flush db: %s, table: %s", database, table), tableThrowable.get());
            }
        } finally {
            lock.unlock();
        }
    }

    public void checkFlushInterval(long chunkId) {
        lock.lock();
        try {
            if (activeChunk == null || activeChunk.getChunkId() != chunkId) {
                return;
            }
            switchChunk(FlushChunkReason.CHUNK_DELAY);
        } finally {
            lock.unlock();
        }
    }

    public long cacheEvict() {
        lock.lock();
        try {
            if (activeChunk == null || activeChunk.numRows() == 0) {
                return 0;
            }
            long size = activeChunk.rowBytes();
            switchChunk(FlushChunkReason.CACHE_EVICT);
            return size;
        } finally {
            lock.unlock();
        }
    }

    private Chunk getActiveChunk() {
        if (activeChunk == null) {
            activeChunk = new Chunk(properties.getDataFormat(), chunkIdGenerator.incrementAndGet());
            if (timer != null) {
                timer.cancel(true);
            }
            timer = streamLoader.scheduleFlush(this, activeChunk.getChunkId(), flushIntervalMs);
        }
        return activeChunk;
    }

    private void switchChunk(FlushChunkReason reason) {
        if (activeChunk == null ||
                (reason == FlushChunkReason.CHUNK_FULL && activeChunk.estimateChunkSize() < chunkSize)) {
            return;
        }

        Chunk inactiveChunk = activeChunk;
        activeChunk = null;
        if (timer != null) {
            timer.cancel(true);
            timer = null;
        }
        if (inactiveChunk.numRows() > 0) {
            flushChunk(inactiveChunk, reason);
        }
    }

    private void flushChunk(Chunk chunk, FlushChunkReason reason) {
        LoadRequest request = new LoadRequest(this, chunk);
        inflightLoadRequests.put(chunk.getChunkId(), request);
        manager.onLoadStart(this, chunk.rowBytes());
        streamLoader.sendLoad(request, 0);
        LOG.info("Flush chunk, db: {}, table: {}, chunkId: {}, rows: {}, bytes: {}, reason: {}",
                database, table, chunk.getChunkId(), chunk.numRows(), chunk.rowBytes(), reason);
    }

    public void loadFinish(LoadRequest request, Throwable throwable) {
        if (throwable != null) {
            request.addThrowable(throwable);
            if (isRetryable(throwable) && request.getRetries() < maxRetries) {
                request.incRetries();
                request.reset();
                streamLoader.sendLoad(request, retryIntervalInMs);
                LOG.warn("Retry to flush chunk, db: {}, table: {}, chunkId: {}, retries: {}, last exception",
                        database, table, request.getChunk().getChunkId(), request.getRetries(), throwable);
                return;
            }

            tableThrowable.compareAndSet(null, throwable);
            manager.onLoadFailure(this, throwable);
            LOG.error("Failed to flush chunk, db: {}, table: {}, chunkId: {}, retries: {}, last exception",
                    database, table, request.getChunk().getChunkId(), request.getRetries(), throwable);
        } else {
            Chunk chunk = request.getChunk();
            cacheBytes.addAndGet(-chunk.rowBytes());
            cacheRows.addAndGet(-chunk.numRows());
            request.getResponse().setFlushBytes(chunk.rowBytes());
            request.getResponse().setFlushRows(chunk.numRows());
            manager.onLoadSuccess(this, request.getResponse());
            LOG.info("Success to flush chunk, db: {}, table: {}, chunkId: {}",
                    database, table, request.getChunk().getChunkId());
        }
        lock.lock();
        try {
            inflightLoadRequests.remove(request.getChunk().getChunkId());
            flushCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public HttpEntity getHttpEntity(Chunk chunk) {
        ChunkHttpEntity entity = new ChunkHttpEntity(String.join(".", database, table), chunk);
        return compressionCodec
                .map(codec -> (HttpEntity) new CompressionHttpEntity(entity, codec))
                .orElse(entity);
    }

    public Optional<CompressionCodec> getCompressionCodec() {
        return compressionCodec;
    }
}
