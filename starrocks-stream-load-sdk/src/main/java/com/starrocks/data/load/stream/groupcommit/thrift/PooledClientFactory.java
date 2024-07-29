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

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class PooledClientFactory  extends BasePooledObjectFactory<ThriftClient> {

    private final String host;
    private final int port;

    public PooledClientFactory(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public ThriftClient create() throws Exception {
        ThriftClient client = new ThriftClient(host, port);
        client.open();
        return client;
    }

    @Override
    public PooledObject<ThriftClient> wrap(ThriftClient obj) {
        return new DefaultPooledObject<>(obj);
    }

    @Override
    public void destroyObject(PooledObject<ThriftClient> p) throws Exception {
        ThriftClient client = p.getObject();
        if (client != null) {
            client.close();
        }
    }

    public boolean validateObject(PooledObject<ThriftClient> p) {
        ThriftClient client = p.getObject();
        return client != null && client.isOpen();
    }
}
