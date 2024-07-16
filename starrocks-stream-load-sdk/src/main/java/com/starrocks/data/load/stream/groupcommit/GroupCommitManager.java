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

import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoadUtils;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.v2.StreamLoadListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GroupCommitManager implements StreamLoadManager, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(GroupCommitManager.class);

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_FLUSH_TIMEOUT_MS = 600000;

    private final StreamLoadProperties properties;
    private final LabelManager labelManager;
    private final GroupCommitStreamLoader streamLoader;
    private final int maxRetries;
    private final int retryIntervalInMs;
    private final int flushIntervalMs;
    private final int flushTimeoutMs;
    private final long maxCacheBytes;
    private final long maxWriteBlockCacheBytes;
    private final Map<TableId, GroupCommitTable> tables = new ConcurrentHashMap<>();
    private final AtomicLong currentCacheBytes = new AtomicLong(0L);
    private final AtomicLong inflightBytes = new AtomicLong(0L);
    private final Lock lock = new ReentrantLock();
    private final Condition writable = lock.newCondition();
    private final AtomicReference<Throwable> exception;
    private transient Thread cacheMonitorThread;
    private transient AtomicBoolean closed;

    public GroupCommitManager(StreamLoadProperties properties) {
        this.properties = properties;
        this.labelManager = new LabelManager(properties);
        this.streamLoader = new GroupCommitStreamLoader(labelManager);
        this.maxRetries = properties.getMaxRetries();
        this.retryIntervalInMs = properties.getRetryIntervalInMs();
        this.flushIntervalMs = (int) properties.getExpectDelayTime();
        String timeout = properties.getHeaders().get("timeout");
        this.flushTimeoutMs = timeout != null ? Integer.parseInt(timeout) * 1000 : DEFAULT_FLUSH_TIMEOUT_MS;
        this.maxCacheBytes = properties.getMaxCacheBytes();
        this.maxWriteBlockCacheBytes = 2 * maxCacheBytes;
        this.exception = new AtomicReference<>();
    }

    @Override
    public void init() {
        this.closed = new AtomicBoolean(false);
        this.labelManager.start();
        this.streamLoader.start(properties, this);
        this.cacheMonitorThread = new Thread(this::monitorCache, "starrocks-cache-monitor");
        this.cacheMonitorThread.setDaemon(true);
        this.cacheMonitorThread.start();
    }

    @Override
    public void write(String uniqueKey, String database, String table, String... rows) {
        GroupCommitTable groupCommitTable = getTable(database, table);
        for (String row : rows) {
            checkException();
            int bytes = groupCommitTable.write(row.getBytes(StandardCharsets.UTF_8));
            currentCacheBytes.addAndGet(bytes);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Write, database {}, table {}, row {}", database, table, row);
            }
            checkCacheFull();
        }
    }

    private void checkCacheFull() {
        long cacheBytes = currentCacheBytes.get();
        if (cacheBytes < maxWriteBlockCacheBytes) {
            return;
        }
        lock.lock();
        try {
            int idx = 0;
            while (currentCacheBytes.get() >= maxWriteBlockCacheBytes) {
                checkException();
                LOG.info("Cache full, wait flush, currentBytes: {}, maxWriteBlockCacheBytes: {}",
                        currentCacheBytes.get(), maxWriteBlockCacheBytes);
                writable.await(Math.min(++idx, 5), TimeUnit.SECONDS);
            }
        } catch (InterruptedException ex) {
            exception.compareAndSet(null, ex);
            throw new RuntimeException(ex);
        } finally {
            lock.unlock();
        }
    }

    private void monitorCache() {
        List<GroupCommitTable> cachedTables = new ArrayList<>();
        while (!closed.get()) {
            long memoryBytes = currentCacheBytes.get() - inflightBytes.get();
            if (memoryBytes >= maxCacheBytes * 3 / 2) {
                if (tables.size() > cachedTables.size()) {
                    cachedTables.clear();
                    cachedTables.addAll(tables.values());
                }
                Collections.shuffle(cachedTables);
                long leftBytes = memoryBytes - maxCacheBytes;
                for (GroupCommitTable table : cachedTables) {
                    if (leftBytes > 0) {
                        leftBytes -= table.forceFlushChunk();
                    }
                }
            }
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public void onLoadStart(GroupCommitTable table, long dataSize) {
        inflightBytes.addAndGet(dataSize);
    }

    public void onLoadSuccess(GroupCommitTable table, StreamLoadResponse response) {
        long cacheByteBeforeFlush = currentCacheBytes.getAndAdd(-response.getFlushBytes());
        inflightBytes.addAndGet(-response.getFlushBytes());
        LOG.info("Receive load response, cacheByteBeforeFlush: {}, currentCacheBytes: {}",
                cacheByteBeforeFlush, currentCacheBytes.get());
        lock.lock();
        try {
            writable.signal();
        } finally {
            lock.unlock();
        }
    }

    public void onLoadFailure(GroupCommitTable table, Throwable throwable) {
        this.exception.compareAndSet(null, throwable);
    }

    public Throwable getException() {
        return exception.get();
    }

    @Override
    public void flush() {
        for (GroupCommitTable table : tables.values()) {
            try {
                table.flush();
            } catch (Exception e) {
                exception.compareAndSet(null, e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() {
        closed.set(true);
        if (cacheMonitorThread != null) {
            cacheMonitorThread.interrupt();
        }
        streamLoader.close();
        labelManager.close();
    }

    private void checkException() {
        if (exception.get() != null) {
            throw new RuntimeException(exception.get());
        }
    }

    private GroupCommitTable getTable(String database, String table) {
        TableId tableId = TableId.of(database, table);
        GroupCommitTable groupCommitTable = tables.get(tableId);
        if (groupCommitTable == null) {
            StreamLoadTableProperties tableProperties = properties.getTableProperties(
                    StreamLoadUtils.getTableUniqueKey(database, table), database, table);
            groupCommitTable = new GroupCommitTable(database, table, this, streamLoader,
                            tableProperties, maxRetries, retryIntervalInMs, flushIntervalMs, flushTimeoutMs,
                            properties.getDefaultTableProperties().getChunkLimit());
            tables.put(tableId, groupCommitTable);
        }
        return groupCommitTable;
    }

    public StreamLoader getStreamLoader() {
        return streamLoader;
    }

    @Override
    public StreamLoadSnapshot snapshot() {
        return new StreamLoadSnapshot();
    }

    @Override
    public boolean prepare(StreamLoadSnapshot snapshot) {
        return true;
    }

    @Override
    public boolean commit(StreamLoadSnapshot snapshot) {
        return true;
    }

    @Override
    public boolean abort(StreamLoadSnapshot snapshot) {
        return true;
    }

    public void setLabelGeneratorFactory(LabelGeneratorFactory labelGeneratorFactory) {
        // ignore
    }

    public void setStreamLoadListener(StreamLoadListener streamLoadListener) {
    }

    @Override
    public void callback(StreamLoadResponse response) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void callback(Throwable e) {
        throw new UnsupportedOperationException();

    }
}

