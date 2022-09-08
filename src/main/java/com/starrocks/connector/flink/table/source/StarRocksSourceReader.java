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

import com.starrocks.connector.flink.row.source.StarRocksSourceFlinkRows;
import com.starrocks.connector.flink.table.source.struct.ColunmRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.tools.StarRocksException;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/** StarRocks source reader. */
public class StarRocksSourceReader implements SourceReader<RowData, StarRocksSplit> {

    private final static Logger LOG = LoggerFactory.getLogger(StarRocksSourceReader.class);

    private static final String TOTAL_SCANNED_ROWS = "totalScannedRows";

    private final StarRocksSourceOptions sourceOptions;
    private final QueryInfo queryInfo;
    private final Long dataCount;
    private final SelectColumn[] selectColumns;
    private final List<ColunmRichInfo> colunmRichInfos;
    private final StarRocksSourceQueryType queryType;
    private final SourceReaderContext readerContext;
    private final CompletableFuture<Void> sourceEventFuture;
    private final List<StarRocksSourceBeBatchReader> dataReaderList;
    private final FutureCompletingBlockingQueue<RowBatch> flinkRowsQueue;
    private final AtomicInteger numFinishedReader;
    private final AtomicReference<Throwable> exception;

    // Lazily initialized
    @Nullable
    private ExecutorService executorService;
    private RowBatch rowBatch;
    private Counter counterTotalScannedRows;

    public StarRocksSourceReader(StarRocksSourceOptions sourceOptions, QueryInfo queryInfo, Long dataCount,
                                 SelectColumn[] selectColumns, List<ColunmRichInfo> colunmRichInfos,
                                 StarRocksSourceQueryType queryType, SourceReaderContext readerContext) {
        this.sourceOptions = sourceOptions;
        this.queryInfo = queryInfo;
        this.dataCount = dataCount;
        this.selectColumns = selectColumns;
        this.colunmRichInfos = colunmRichInfos;
        this.queryType = queryType;
        this.readerContext = readerContext;
        this.sourceEventFuture = new CompletableFuture<>();
        this.dataReaderList = new ArrayList<>();
        this.flinkRowsQueue = new FutureCompletingBlockingQueue<>(sourceOptions.getScanReaderQueueCapacity());
        this.numFinishedReader = new AtomicInteger();
        this.exception = new AtomicReference<>();
        LOG.info("Create source reader {}", readerContext.getIndexOfSubtask());
    }

    @Override
    public void start() {
        LOG.info("Start source reader {}", readerContext.getIndexOfSubtask());
        this.counterTotalScannedRows = readerContext.metricGroup().counter(TOTAL_SCANNED_ROWS);
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        checkException();
        if (rowBatch == null || !rowBatch.hasNext()) {
            rowBatch = flinkRowsQueue.poll();
            if (rowBatch == RowBatch.EOF) {
                return InputStatus.END_OF_INPUT;
            }
        }

        if (rowBatch != null && rowBatch.hasNext()) {
            output.collect(rowBatch.next());
            counterTotalScannedRows.inc(1);
            return InputStatus.MORE_AVAILABLE;
        }

        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        checkException();
        return !sourceEventFuture.isDone() ? sourceEventFuture :
                (rowBatch != null
                    ? FutureCompletingBlockingQueue.AVAILABLE
                    : flinkRowsQueue.getAvailabilityFuture());
    }

    @Override
    public void addSplits(List<StarRocksSplit> splits) {
    }

    @Override
    public void notifyNoMoreSplits() {
    }

    @Override
    public List<StarRocksSplit> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        LOG.info("Receive source event {}", sourceEvent);
        if (sourceEvent instanceof StarRocksSourceEvent) {
            if (this.queryType == StarRocksSourceQueryType.QueryCount) {
                startQueryCountReader();
            } else {
                startNonQueryCountReader(((StarRocksSourceEvent) sourceEvent).getParallelism());
            }
            sourceEventFuture.complete(null);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close source reader");
        if (executorService != null) {
            executorService.shutdownNow();
        }
        this.dataReaderList.parallelStream().forEach(dataReader -> {
            if (dataReader != null) {
                dataReader.close();
            }
        });
    }

    private void startQueryCountReader() {
        LOG.info("Start query count reader");
        int subTaskId = readerContext.getIndexOfSubtask();
        if (subTaskId == 0 && this.dataCount > 0) {
            rowBatch = new RowBatch(new Iterator<GenericRowData>() {

                private long count = StarRocksSourceReader.this.dataCount;

                @Override
                public boolean hasNext() {
                    return count > 0;
                }

                @Override
                public GenericRowData next() {
                    count--;
                    return new GenericRowData(0);
                }
            });
        }

        try {
            flinkRowsQueue.put(0, RowBatch.EOF);
        } catch (Exception e) {
            throw new StarRocksException(e);
        }
    }

    private void startNonQueryCountReader(int parallelism) {
        LOG.info("Start non query count reader");
        int subTaskId = readerContext.getIndexOfSubtask();
        List<List<QueryBeXTablets>> lists = StarRocksSourceCommonFunc.splitQueryBeXTablets(parallelism, queryInfo);
        lists.get(subTaskId).forEach(beXTablets -> {
            StarRocksSourceBeBatchReader beReader =
                    new StarRocksSourceBeBatchReader(beXTablets.getBeNode(), colunmRichInfos, selectColumns, sourceOptions);
            beReader.openScanner(beXTablets.getTabletIds(), queryInfo.getQueryPlan().getOpaqued_query_plan(), sourceOptions);
            this.dataReaderList.add(beReader);
        });

        this.executorService = Executors.newFixedThreadPool(
                sourceOptions.getScanReaderNumConcurrentFetcher(), r -> new Thread(r, "Source Reader for " + subTaskId));
        final AtomicInteger count = new AtomicInteger(0);
        this.dataReaderList.forEach(dataReader ->
            executorService.submit(() -> {
                int index = count.incrementAndGet();
                while (true) {
                    try {
                        StarRocksSourceFlinkRows rows = dataReader.getNextBatch();
                        if (rows == null) {
                            if (numFinishedReader.incrementAndGet() == dataReaderList.size()) {
                                // insert an EOF to indicate the InputStatus.END_OF_INPUT
                                flinkRowsQueue.put(index, RowBatch.EOF);
                            }
                            return;
                        }

                        flinkRowsQueue.put(index, new RowBatch(rows));
                    } catch (Exception e) {
                        exception.compareAndSet(null, e);
                        LOG.error("Failed to read flink rows", e);
                        return;
                    }
                }
            }
        ));

        if (dataReaderList.isEmpty()) {
            try {
                flinkRowsQueue.put(0, RowBatch.EOF);
            } catch (Exception e) {
                throw new StarRocksException(e);
            }
        }
    }

    private void checkException() {
        if (exception.get() != null) {
            throw new StarRocksException(exception.get());
        }
    }

    private static class RowBatch implements Iterator<GenericRowData> {

        private static final RowBatch EOF = new RowBatch(Collections.emptyIterator());

        private final Iterator<GenericRowData> delegateIterator;

        public RowBatch(Iterator<GenericRowData> delegateIterator) {
            this.delegateIterator = delegateIterator;
        }

        @Override
        public boolean hasNext() {
            return delegateIterator.hasNext();
        }

        @Override
        public GenericRowData next() {
            return delegateIterator.next();
        }
    }
}
