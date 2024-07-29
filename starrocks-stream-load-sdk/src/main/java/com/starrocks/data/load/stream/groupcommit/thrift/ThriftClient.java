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

import com.starrocks.thrift.TFrontendService;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThriftClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ThriftClient.class);

    private final String host;
    private final int port;
    private final AtomicBoolean closed;
    private String localHost;
    private int localPort;
    private TSocket socket;
    private TFrontendService.Client service;

    public ThriftClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.closed = new AtomicBoolean(false);
    }

    void open() throws Exception {
        this.socket = new TSocket(TConfiguration.DEFAULT, host, port, 60000);
        try {
            socket.open();
            this.localHost = socket.getSocket().getLocalAddress().getHostAddress();
            this.localPort = socket.getSocket().getLocalPort();
            TProtocol protocol = new TBinaryProtocol.Factory().getProtocol(socket);
            service = new TFrontendService.Client(protocol);
            LOG.info("Open thrift client, local: {}:{}, remote: {}:{}", getLocalAddress(), getLocalPort(), host, port);
        } catch (TTransportException e) {
            socket.close();
            LOG.error("Failed to open thrift client, {}:{}", host, port, e);
            throw new RuntimeException(
                String.format("Failed to open socket, %s:%s", host, port), e);
        }
    }

    public boolean isOpen() {
        return socket != null && socket.isOpen();
    }

    public TFrontendService.Client getService() {
        return service;
    }

    public String getId() {
        return getLocalAddress() + ":" + getLocalPort();
    }

    public String getLocalAddress() {
        return localHost;
    }

    public int getLocalPort() {
        return localPort;
    }

    @Override
    public void close() throws IOException {
      if (!closed.compareAndSet(false, true)) {
        return;
      }

        if (socket != null) {
            try {
                socket.close();
                LOG.info("Close thrift client, local: {}, remote: {}:{}", getId(), host, port);
            } catch (Exception e) {
                LOG.warn("Failed to close thrift client, remote: {}:{}", host, port, e);
            } finally {
                socket = null;
            }
        }
    }
}
