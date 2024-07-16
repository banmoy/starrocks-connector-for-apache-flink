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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadUtils;
import com.starrocks.data.load.stream.TransactionStatus;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LabelUtils {

    private static final Logger LOG = LoggerFactory.getLogger(LabelUtils.class);

    public static TransactionStatus getLabelStaus(
            String host, String username, String password, String database,
            String label, ObjectMapper objectMapper) throws Exception {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            String url = host + "/api/" + database + "/get_load_state?label=" + label;
            HttpGet httpGet = new HttpGet(url);
            httpGet.addHeader("Authorization", StreamLoadUtils.getBasicAuthHeader(username, password));
            httpGet.setHeader("Connection", "close");
            try (CloseableHttpResponse response = client.execute(httpGet)) {
                int responseStatusCode = response.getStatusLine().getStatusCode();
                String entityContent = EntityUtils.toString(response.getEntity());
                LOG.debug("Response for get_load_state, label: {}, response status code: {}, response body : {}",
                        label, responseStatusCode, entityContent);
                if (responseStatusCode != 200) {
                    throw new StreamLoadFailException(String.format("Could not get load state because of incorrect response status code %s, " +
                            "label: %s, response body: %s", responseStatusCode, label, entityContent));
                }

                StreamLoadResponse.StreamLoadResponseBody responseBody =
                        objectMapper.readValue(entityContent, StreamLoadResponse.StreamLoadResponseBody.class);
                String state = responseBody.getState();
                if (state == null) {
                    throw new StreamLoadFailException(String.format("Could not get load state because of state is null," +
                            "label: %s, load information: %s", label, entityContent));
                }

                return TransactionStatus.valueOf(state.toUpperCase());
            }
        }
    }
}
