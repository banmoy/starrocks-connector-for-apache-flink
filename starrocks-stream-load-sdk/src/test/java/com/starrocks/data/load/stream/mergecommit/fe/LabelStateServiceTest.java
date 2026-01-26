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

package com.starrocks.data.load.stream.mergecommit.fe;

import com.starrocks.data.load.stream.TransactionStatus;
import com.starrocks.data.load.stream.mergecommit.Pair;
import com.starrocks.data.load.stream.mergecommit.TableId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LabelStateServiceTest {

    private ScheduledExecutorService executorService;
    private FeHttpService mockHttpService;
    private LabelStateService labelStateService;

    @Before
    public void setUp() {
        executorService = Executors.newScheduledThreadPool(2);
        mockHttpService = mock(FeHttpService.class);
        labelStateService = new LabelStateService(mockHttpService, executorService);
        labelStateService.start();
    }

    @After
    public void tearDown() {
        if (labelStateService != null) {
            labelStateService.close();
        }
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    private String createLabelResponse(String state) {
        return String.format("{\"status\":\"OK\",\"state\":\"%s\"}", state);
    }

    @Test
    public void testWaitForLabelFinalStateReturnsVisible() throws Exception {
        when(mockHttpService.getLabelState(anyString(), anyString()))
                .thenReturn(Pair.of(200, createLabelResponse("VISIBLE")));

        TableId tableId = TableId.of("test_db", "test_table");
        CompletableFuture<LabelStateService.LabelMeta> future = labelStateService.waitForLabelFinalState(
                tableId, "test_label", 0, 100, 5000, -1);

        LabelStateService.LabelMeta result = future.get(5, TimeUnit.SECONDS);

        assertEquals(TransactionStatus.VISIBLE, result.transactionStatus);
    }

    @Test
    public void testWaitForLabelFinalStateReturnsAborted() throws Exception {
        when(mockHttpService.getLabelState(anyString(), anyString()))
                .thenReturn(Pair.of(200, createLabelResponse("ABORTED")));

        TableId tableId = TableId.of("test_db", "test_table");
        CompletableFuture<LabelStateService.LabelMeta> future = labelStateService.waitForLabelFinalState(
                tableId, "test_label_aborted", 0, 100, 5000, -1);

        LabelStateService.LabelMeta result = future.get(5, TimeUnit.SECONDS);

        assertEquals(TransactionStatus.ABORTED, result.transactionStatus);
    }

    @Test
    public void testWaitForLabelFinalStateCommittedPublishTimeout() throws Exception {
        when(mockHttpService.getLabelState(anyString(), anyString()))
                .thenReturn(Pair.of(200, createLabelResponse("COMMITTED")));

        TableId tableId = TableId.of("test_db", "test_table");
        CompletableFuture<LabelStateService.LabelMeta> future = labelStateService.waitForLabelFinalState(
                tableId, "test_label_committed", 0, 50, 10000, 100);

        LabelStateService.LabelMeta result = future.get(5, TimeUnit.SECONDS);

        assertEquals(TransactionStatus.COMMITTED, result.transactionStatus);
    }

    @Test
    public void testWaitForLabelFinalStateCommittedEventuallyVisible() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        when(mockHttpService.getLabelState(anyString(), anyString()))
                .thenAnswer(invocation -> {
                    int count = callCount.incrementAndGet();
                    if (count <= 2) {
                        return Pair.of(200, createLabelResponse("COMMITTED"));
                    } else {
                        return Pair.of(200, createLabelResponse("VISIBLE"));
                    }
                });

        TableId tableId = TableId.of("test_db", "test_table");
        CompletableFuture<LabelStateService.LabelMeta> future = labelStateService.waitForLabelFinalState(
                tableId, "test_label_committed_visible", 0, 50, 10000, -1);

        LabelStateService.LabelMeta result = future.get(5, TimeUnit.SECONDS);

        assertEquals(TransactionStatus.VISIBLE, result.transactionStatus);
        assertTrue(callCount.get() >= 3);
    }

    @Test
    public void testWaitForLabelFinalStateCheckLabelStateTimeout() throws Exception {
        when(mockHttpService.getLabelState(anyString(), anyString()))
                .thenReturn(Pair.of(200, createLabelResponse("PREPARE")));

        TableId tableId = TableId.of("test_db", "test_table");
        CompletableFuture<LabelStateService.LabelMeta> future = labelStateService.waitForLabelFinalState(
                tableId, "test_label_timeout", 0, 50, 200, -1);

        try {
            future.get(5, TimeUnit.SECONDS);
            fail("Expected ExecutionException due to timeout");
        } catch (ExecutionException e) {
            assertTrue(e.getCause().getMessage().contains("Check label state timeout"));
        }
    }

    @Test
    public void testWaitForLabelFinalStateServiceClosed() throws Exception {
        labelStateService.close();

        TableId tableId = TableId.of("test_db", "test_table");
        CompletableFuture<LabelStateService.LabelMeta> future = labelStateService.waitForLabelFinalState(
                tableId, "test_label_closed", 0, 100, 5000, -1);

        try {
            future.get(1, TimeUnit.SECONDS);
            fail("Expected ExecutionException due to closed service");
        } catch (ExecutionException e) {
            assertTrue(e.getCause().getMessage().contains("LabelStateService is closed"));
        }
    }

    @Test
    public void testWaitForLabelFinalStateHttpError() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        when(mockHttpService.getLabelState(anyString(), anyString()))
                .thenAnswer(invocation -> {
                    int count = callCount.incrementAndGet();
                    if (count <= 1) {
                        return Pair.of(500, "Internal Server Error");
                    } else {
                        return Pair.of(200, createLabelResponse("VISIBLE"));
                    }
                });

        TableId tableId = TableId.of("test_db", "test_table");
        CompletableFuture<LabelStateService.LabelMeta> future = labelStateService.waitForLabelFinalState(
                tableId, "test_label_http_error", 0, 50, 5000, -1);

        LabelStateService.LabelMeta result = future.get(5, TimeUnit.SECONDS);

        assertEquals(TransactionStatus.VISIBLE, result.transactionStatus);
        assertTrue(callCount.get() >= 2);
    }

    @Test
    public void testWaitForLabelFinalStatePublishTimeoutDisabledWhenNegative() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        when(mockHttpService.getLabelState(anyString(), anyString()))
                .thenAnswer(invocation -> {
                    int count = callCount.incrementAndGet();
                    if (count <= 3) {
                        return Pair.of(200, createLabelResponse("COMMITTED"));
                    } else {
                        return Pair.of(200, createLabelResponse("VISIBLE"));
                    }
                });

        TableId tableId = TableId.of("test_db", "test_table");
        CompletableFuture<LabelStateService.LabelMeta> future = labelStateService.waitForLabelFinalState(
                tableId, "test_label_no_publish_timeout", 0, 50, 10000, -1);

        LabelStateService.LabelMeta result = future.get(5, TimeUnit.SECONDS);

        assertEquals(TransactionStatus.VISIBLE, result.transactionStatus);
    }

    @Test
    public void testWaitForLabelFinalStatePublishTimeoutDisabledWhenZero() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        when(mockHttpService.getLabelState(anyString(), anyString()))
                .thenAnswer(invocation -> {
                    int count = callCount.incrementAndGet();
                    if (count <= 3) {
                        return Pair.of(200, createLabelResponse("COMMITTED"));
                    } else {
                        return Pair.of(200, createLabelResponse("VISIBLE"));
                    }
                });

        TableId tableId = TableId.of("test_db", "test_table");
        CompletableFuture<LabelStateService.LabelMeta> future = labelStateService.waitForLabelFinalState(
                tableId, "test_label_zero_publish_timeout", 0, 50, 10000, 0);

        LabelStateService.LabelMeta result = future.get(5, TimeUnit.SECONDS);

        assertEquals(TransactionStatus.VISIBLE, result.transactionStatus);
    }
}
