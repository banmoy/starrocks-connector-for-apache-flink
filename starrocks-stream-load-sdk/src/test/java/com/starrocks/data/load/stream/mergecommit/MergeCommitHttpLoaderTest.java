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

package com.starrocks.data.load.stream.mergecommit;

import com.starrocks.data.load.stream.StreamLoadConstants;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.TransactionStatus;
import com.starrocks.data.load.stream.mergecommit.fe.LabelStateService;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link MergeCommitHttpLoader} status handling logic.
 * 
 * Tests the completeAsyncMode behavior through the Table.loadFinish flow,
 * verifying that VISIBLE and COMMITTED statuses are treated as success.
 */
public class MergeCommitHttpLoaderTest {

    private MergeCommitManager manager;
    private MergeCommitLoader loader;
    private SendLoadSpy sendLoadSpy;
    private AtomicReference<Throwable> capturedError;
    private CountDownLatch loadFinishLatch;

    @FunctionalInterface
    private interface SendLoadObserver {
        void onSend(LoadRequest.RequestRun requestRun, int count);
    }

    private static class SendLoadSpy {
        private final AtomicInteger requestCount = new AtomicInteger(0);
        private volatile SendLoadObserver observer = (requestRun, count) -> {};

        public void setObserver(SendLoadObserver observer) {
            this.observer = observer == null ? (requestRun, count) -> {} : observer;
        }

        public void install(MergeCommitLoader loader) {
            doAnswer(invocation -> {
                LoadRequest.RequestRun requestRun = invocation.getArgument(0);
                int count = requestCount.incrementAndGet();
                observer.onSend(requestRun, count);
                return null;
            }).when(loader).sendLoad(any(), anyInt());
        }
    }

    @Before
    public void setUp() {
        capturedError = new AtomicReference<>();
        loadFinishLatch = new CountDownLatch(1);

        manager = mock(MergeCommitManager.class);
        loader = mock(MergeCommitLoader.class);
        sendLoadSpy = new SendLoadSpy();

        doNothing().when(manager).onLoadStart(any(), anyLong(), anyInt());
        doNothing().when(manager).releaseCache(any());

        doAnswer(invocation -> {
            loadFinishLatch.countDown();
            return null;
        }).when(manager).onLoadSuccess(any(), any());

        doAnswer(invocation -> {
            Throwable throwable = invocation.getArgument(2);
            capturedError.set(throwable);
            loadFinishLatch.countDown();
            return null;
        }).when(manager).onLoadFailure(any(), any(), any());

        doReturn(mock(ScheduledFuture.class))
                .when(loader).scheduleFlush(any(), anyLong(), anyInt());

        sendLoadSpy.install(loader);
    }

    private Table createTableWithMockito(int maxConcurrentRequests) {
        StreamLoadTableProperties props = StreamLoadTableProperties.builder()
                .database("test_db")
                .table("test_tbl")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .chunkLimit(100)
                .addProperty("enable_merge_commit", "true")
                .addProperty("merge_commit_async", "true")
                .build();
        return new Table(
                "test_db", "test_tbl", manager, loader, props,
                0,      // maxRetries
                0,      // retryIntervalInMs
                10000,  // flushIntervalMs
                100,    // chunkSize
                maxConcurrentRequests);
    }

    private void writeDataToTriggerFlush(Table table) {
        byte[] data = new byte[150];
        table.write(data);
    }

    // ==================== completeAsyncMode behavior tests ====================

    /**
     * Test that completeAsyncMode treats VISIBLE transaction status as success.
     * 
     * This tests the actual completeAsyncMode method in MergeCommitHttpLoader,
     * verifying that when LabelStateService returns VISIBLE status, the load
     * is completed successfully (loadFinish called with null error).
     */
    @Test
    public void testLoadFinishVisibleStatusSuccess() throws Exception {
        Table table = createTableWithMockito(-1);

        sendLoadSpy.setObserver((requestRun, count) -> {
            StreamLoadResponse.StreamLoadResponseBody responseBody = new StreamLoadResponse.StreamLoadResponseBody();
            responseBody.setStatus(StreamLoadConstants.RESULT_STATUS_SUCCESS);
            responseBody.setLabel("test-label");
            StreamLoadResponse response = new StreamLoadResponse();
            response.setBody(responseBody);
            requestRun.loadResult = response;

            // Simulate completeAsyncMode behavior with VISIBLE status
            LabelStateService.LabelMeta labelMeta = createLabelMeta(TransactionStatus.VISIBLE, null);
            invokeCompleteAsyncMode(requestRun, labelMeta, null);
        });

        writeDataToTriggerFlush(table);

        assertTrue("Load should complete within timeout", loadFinishLatch.await(5, TimeUnit.SECONDS));
        assertNull("VISIBLE status should not produce an error", capturedError.get());
    }

    /**
     * Test that completeAsyncMode treats COMMITTED transaction status as success.
     * 
     * This is a key change in the publish timeout feature - COMMITTED is now acceptable.
     * When LabelStateService returns COMMITTED status (e.g., after publish timeout),
     * the load should be completed successfully instead of being treated as an error.
     */
    @Test
    public void testLoadFinishCommittedStatusSuccess() throws Exception {
        Table table = createTableWithMockito(-1);

        sendLoadSpy.setObserver((requestRun, count) -> {
            StreamLoadResponse.StreamLoadResponseBody responseBody = new StreamLoadResponse.StreamLoadResponseBody();
            responseBody.setStatus(StreamLoadConstants.RESULT_STATUS_SUCCESS);
            responseBody.setLabel("test-label");
            StreamLoadResponse response = new StreamLoadResponse();
            response.setBody(responseBody);
            requestRun.loadResult = response;

            // Simulate completeAsyncMode behavior with COMMITTED status
            // This exercises the new code path where COMMITTED is treated as success
            LabelStateService.LabelMeta labelMeta = createLabelMeta(TransactionStatus.COMMITTED, null);
            invokeCompleteAsyncMode(requestRun, labelMeta, null);
        });

        writeDataToTriggerFlush(table);

        assertTrue("Load should complete within timeout", loadFinishLatch.await(5, TimeUnit.SECONDS));
        assertNull("COMMITTED status should not produce an error", capturedError.get());
    }

    /**
     * Helper method to create a LabelMeta with specified transaction status.
     */
    private LabelStateService.LabelMeta createLabelMeta(TransactionStatus status, String reason) {
        try {
            // Use reflection to create LabelMeta since it has package-private constructor
            java.lang.reflect.Constructor<LabelStateService.LabelMeta> constructor =
                    LabelStateService.LabelMeta.class.getDeclaredConstructor(
                            TableId.class, String.class, int.class, int.class, int.class, int.class);
            constructor.setAccessible(true);
            LabelStateService.LabelMeta labelMeta = constructor.newInstance(
                    TableId.of("test_db", "test_tbl"), "test-label", 0, 1000, 60000, 0);
            labelMeta.transactionStatus = status;
            labelMeta.reason = reason;
            return labelMeta;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create LabelMeta", e);
        }
    }

    /**
     * Helper method to invoke the actual completeAsyncMode method in MergeCommitHttpLoader
     * via reflection. This ensures tests exercise the real production code rather than
     * a reimplementation of the logic.
     */
    private void invokeCompleteAsyncMode(LoadRequest.RequestRun requestRun,
                                         LabelStateService.LabelMeta labelMeta,
                                         Throwable throwable) {
        try {
            MergeCommitHttpLoader httpLoader = new MergeCommitHttpLoader();
            java.lang.reflect.Method method = MergeCommitHttpLoader.class.getDeclaredMethod(
                    "completeAsyncMode",
                    LoadRequest.RequestRun.class,
                    LabelStateService.LabelMeta.class,
                    Throwable.class);
            method.setAccessible(true);
            method.invoke(httpLoader, requestRun, labelMeta, throwable);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke completeAsyncMode", e);
        }
    }

    /**
     * Test that completeAsyncMode treats ABORTED transaction status as an error.
     * 
     * When LabelStateService returns ABORTED status, the load should fail
     * with an appropriate error message.
     */
    @Test
    public void testLoadFinishAbortedStatusError() throws Exception {
        Table table = createTableWithMockito(-1);

        sendLoadSpy.setObserver((requestRun, count) -> {
            StreamLoadResponse.StreamLoadResponseBody responseBody = new StreamLoadResponse.StreamLoadResponseBody();
            responseBody.setStatus(StreamLoadConstants.RESULT_STATUS_SUCCESS);
            responseBody.setLabel("test-label");
            StreamLoadResponse response = new StreamLoadResponse();
            response.setBody(responseBody);
            requestRun.loadResult = response;

            // Simulate completeAsyncMode behavior with ABORTED status
            LabelStateService.LabelMeta labelMeta = createLabelMeta(TransactionStatus.ABORTED, "test abort reason");
            invokeCompleteAsyncMode(requestRun, labelMeta, null);
        });

        writeDataToTriggerFlush(table);

        assertTrue("Load should complete within timeout", loadFinishLatch.await(5, TimeUnit.SECONDS));
        assertNotNull("ABORTED status should produce an error", capturedError.get());
        assertTrue(capturedError.get().getMessage().contains("ABORTED"));
    }

    /**
     * Test that completeAsyncMode treats UNKNOWN transaction status as an error.
     * 
     * When LabelStateService returns UNKNOWN status, the load should fail
     * with an appropriate error message.
     */
    @Test
    public void testLoadFinishUnknownStatusError() throws Exception {
        Table table = createTableWithMockito(-1);

        sendLoadSpy.setObserver((requestRun, count) -> {
            StreamLoadResponse.StreamLoadResponseBody responseBody = new StreamLoadResponse.StreamLoadResponseBody();
            responseBody.setStatus(StreamLoadConstants.RESULT_STATUS_SUCCESS);
            responseBody.setLabel("test-label");
            StreamLoadResponse response = new StreamLoadResponse();
            response.setBody(responseBody);
            requestRun.loadResult = response;

            // Simulate completeAsyncMode behavior with UNKNOWN status
            LabelStateService.LabelMeta labelMeta = createLabelMeta(TransactionStatus.UNKNOWN, "test unknown reason");
            invokeCompleteAsyncMode(requestRun, labelMeta, null);
        });

        writeDataToTriggerFlush(table);

        assertTrue("Load should complete within timeout", loadFinishLatch.await(5, TimeUnit.SECONDS));
        assertNotNull("UNKNOWN status should produce an error", capturedError.get());
        assertTrue(capturedError.get().getMessage().contains("UNKNOWN"));
    }

    /**
     * Test that when load completes with PUBLISH_TIMEOUT response status,
     * it is still treated as success (RPC level success).
     */
    @Test
    public void testLoadFinishPublishTimeoutResponseStatusSuccess() throws Exception {
        Table table = createTableWithMockito(-1);

        sendLoadSpy.setObserver((requestRun, count) -> {
            StreamLoadResponse.StreamLoadResponseBody responseBody = new StreamLoadResponse.StreamLoadResponseBody();
            responseBody.setStatus(StreamLoadConstants.RESULT_STATUS_TRANSACTION_PUBLISH_TIMEOUT);
            responseBody.setLabel("test-label");
            StreamLoadResponse response = new StreamLoadResponse();
            response.setBody(responseBody);
            requestRun.loadResult = response;

            table.loadFinish(requestRun, null);
        });

        writeDataToTriggerFlush(table);

        assertTrue("Load should complete within timeout", loadFinishLatch.await(5, TimeUnit.SECONDS));
        assertNull("PUBLISH_TIMEOUT response status should be treated as success", capturedError.get());
    }
}
