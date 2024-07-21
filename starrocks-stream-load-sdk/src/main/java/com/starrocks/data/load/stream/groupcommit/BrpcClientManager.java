package com.starrocks.data.load.stream.groupcommit;

import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcCallback;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.channel.ChannelType;
import com.baidu.brpc.client.channel.Endpoint;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class BrpcClientManager extends SharedService<BrpcClientManager.BrpcConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(BrpcClientManager.class);

    private static final BrpcClientManager INSTANCE = new BrpcClientManager();

    private final ConcurrentHashMap<WorkerAddress, BrpcEndpoint> endpointMap;
    private volatile RpcClientOptions clientOptions;

    public BrpcClientManager() {
        this.endpointMap = new ConcurrentHashMap<>();
    }

    public static BrpcClientManager getInstance() {
        return INSTANCE;
    }

    @Override
    protected void init(BrpcConfig brpcConfig) {
        this.clientOptions = brpcConfig.clientOptions;
        LOG.info("Init brpc client manager");
    }

    @Override
    protected void reset() {
        endpointMap.values().forEach(endpoint -> endpoint.client.stop());
        endpointMap.clear();
        LOG.info("Reset brpc client manager");
    }

    public PBackendServiceAsync getBackendService(WorkerAddress address) {
        return endpointMap.computeIfAbsent(address, this::createEndpoint).service;
    }

    private BrpcEndpoint createEndpoint(WorkerAddress address) {
        try {
            Endpoint endpoint = new Endpoint(address.host, Integer.parseInt(address.port));
            RpcClient rpcClient = new RpcClient(endpoint, clientOptions);
            PBackendServiceAsync service = BrpcProxy.getProxy(rpcClient, PBackendServiceAsync.class);
            LOG.info("Create brpc client, {}", address);
            return new BrpcEndpoint(address, rpcClient, service);
        } catch (Exception e) {
            LOG.error("Failed to create brpc client, {}", address, e);
            throw e;
        }
    }


    private static class BrpcEndpoint {
        WorkerAddress address;
        RpcClient client;
        PBackendServiceAsync service;

        public BrpcEndpoint(WorkerAddress address, RpcClient client, PBackendServiceAsync service) {
            this.address = address;
            this.client = client;
            this.service = service;
        }
    }

    public static class BrpcConfig {
        RpcClientOptions clientOptions;

        public BrpcConfig(RpcClientOptions clientOptions) {
            this.clientOptions = clientOptions;
        }
    }

    public static void main(String[] args) throws Exception {
        RpcClientOptions clientOptions = new RpcClientOptions();
        clientOptions.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        clientOptions.setConnectTimeoutMillis(1000);
        clientOptions.setReadTimeoutMillis(1000);
        clientOptions.setWriteTimeoutMillis(1000);
        clientOptions.setChannelType(ChannelType.POOLED_CONNECTION);
        clientOptions.setMaxTotalConnections(10);
        clientOptions.setMinIdleConnections(10);
        clientOptions.setMaxTryTimes(3);
        clientOptions.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
        clientOptions.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
        clientOptions.setIoThreadNum(Runtime.getRuntime().availableProcessors());
        clientOptions.setWorkThreadNum(Runtime.getRuntime().availableProcessors());
        BrpcClientManager.BrpcConfig brpcConfig = new BrpcClientManager.BrpcConfig(clientOptions);
        BrpcClientManager.getInstance().takeRef(brpcConfig);
        WorkerAddress address = new WorkerAddress("127.0.0.1", "11914");
        PBackendServiceAsync service = BrpcClientManager.getInstance().getBackendService(address);

        PGroupCommitLoadRequest request = new PGroupCommitLoadRequest();
        request.setDb("test");
        request.setTable("t");
        request.setUserLabel("label1");
        request.setTimeout(10);
        request.setClientTimeMs(System.currentTimeMillis());
        byte[] data = "{\"c0\":2}".getBytes();
        RpcContext.getContext().setRequestBinaryAttachment(data);
        Future<PGroupCommitLoadResponse> future = service.groupCommitLoad(request, new TestRpcCallback());
        PGroupCommitLoadResponse response = future.get();
        System.out.println("Send load " + response);
    }

    private static class TestRpcCallback implements RpcCallback<PGroupCommitLoadResponse> {

        @Override
        public void success(PGroupCommitLoadResponse response) {
            System.out.println("Success to receive response");
        }

        @Override
        public void fail(Throwable throwable) {
            System.out.println("Failed to receive response");
            throwable.printStackTrace();
        }
    }
}
