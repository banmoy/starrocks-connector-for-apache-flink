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

package com.starrocks.flink.cdc.sync;

import org.junit.Test;

public class JobTest {

    @Test
    public void testBasic() throws Exception {
        CdcSync.main(getDebugArgs());
    }

    private static String[] getDebugArgs() {
        String args =
                "--src-type mysql " +
                        "--src-conf hostname=127.0.0.1 " +
                        "--src-conf username=root " +
                        "--src-conf password=12345678 " +
                        "--src-conf database-name=cdc_mul_tbl " +
                        "--src-conf server-time-zone=+00:00 " +
                        "--starrocks-db cdc_mul_tbl " +
                        "--sink-conf jdbc-url=jdbc:mysql://127.0.0.1:11903 " +
                        "--sink-conf load-url=127.0.0.1:11901 " +
                        "--sink-conf username=root " +
                        "--sink-conf password= " +
                        "--sink-conf sink.parallelism=2 " +
                        "--sink-conf sink.buffer-flush.interval-ms=5000 " +
                        "--table-conf num-buckets=4 " +
                        "--table-conf properties.replication_num=1";
        return args.split(" ");
    }
}
