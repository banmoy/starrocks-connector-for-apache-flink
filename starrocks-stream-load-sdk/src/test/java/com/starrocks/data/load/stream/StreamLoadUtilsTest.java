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

package com.starrocks.data.load.stream;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamLoadUtilsTest {

    @Test
    public void testGetErrorLogUrlFromTxnAbortReason() {
        String abortedReasonForSr35 = "There is data quality issue, please check the tracking url for details. Max filter ratio: 0.0. " +
                "The tracking url: http://127.0.0.1:8040/api/_load_error_log?file=error_log_19bbc3f6ae0754f_932e963c5ec44399";
        String abortedReasonForSr40 = "There is a data quality issue. Please check the tracking URL or SQL for details. " +
                "Tracking URL: http://127.0.0.1:8040/api/_load_error_log?file=error_log_19bbc3f6ae0754f_932e963c5ec44399. " +
                "Tracking SQL: SELECT tracking_log FROM information_schema.load_tracking_logs WHERE JOB_ID=12345";
        String expectedUrl = "http://127.0.0.1:8040/api/_load_error_log?file=error_log_19bbc3f6ae0754f_932e963c5ec44399";
        Optional<String> url35 = StreamLoadUtils.tryGetErrorLogUrlFromTxnAbortReason(abortedReasonForSr35);
        assertTrue(url35.isPresent());
        assertEquals(expectedUrl, url35.get());
        Optional<String> url40 = StreamLoadUtils.tryGetErrorLogUrlFromTxnAbortReason(abortedReasonForSr40);
        assertTrue(url40.isPresent());
        assertEquals(expectedUrl, url40.get());
        Optional<String> noUrl = StreamLoadUtils.tryGetErrorLogUrlFromTxnAbortReason("no url");
        assertFalse(noUrl.isPresent());
    }
}
