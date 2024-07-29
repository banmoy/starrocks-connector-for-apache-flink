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

package com.starrocks.data.load.stream.groupcommit.thrift;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public class ThriftClientPool implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ThriftClientPool.class);

    private final GenericObjectPool<ThriftClient> clientPool;

    public ThriftClientPool(String host, int port, int maxClient) {

        GenericObjectPoolConfig conf = new GenericObjectPoolConfig();
        conf.setMaxWaitMillis(60000);
        conf.setMaxTotal(maxClient);
        conf.setMaxIdle(maxClient);
        conf.setMinIdle(maxClient);
        conf.setTestWhileIdle(true);
        conf.setTimeBetweenEvictionRunsMillis(5 * 50 * 1000);
        this.clientPool = new GenericObjectPool<>(new PooledClientFactory(host, port), conf);
    }

    public ThriftClient borrowClient() throws Exception {
        return clientPool.borrowObject();
    }

    public void returnClient(ThriftClient client) {
        try {
            clientPool.returnObject(client);
        } catch (Exception e) {
            LOG.warn("Failed to return client: {}", client.getId(), e);
        }
    }

    public void removeClient(ThriftClient client) {
        String id = client.getId();
        try {
            clientPool.invalidateObject(client);
            LOG.info("Success to remove client: {}", id);
        } catch (Exception e) {
            LOG.warn("Failed to remove client: {}", id, e);
        }
    }

    @Override
    public void close() {
        clientPool.close();
    }
}
