package com.starrocks.data.load.stream.mergecommit.be;

import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcCallback;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.channel.ChannelType;
import com.baidu.brpc.client.channel.Endpoint;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;
import com.starrocks.data.load.stream.mergecommit.SharedService;
import com.starrocks.data.load.stream.mergecommit.WorkerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class BackendBrpcService extends SharedService {

    private static final Logger LOG = LoggerFactory.getLogger(BackendBrpcService.class);

    private static volatile BackendBrpcService INSTANCE;

    private final BrpcConfig config;
    private final ConcurrentHashMap<WorkerAddress, BrpcEndpoint> endpointMap;

    public BackendBrpcService(BrpcConfig config) {
        this.config = config;
        this.endpointMap = new ConcurrentHashMap<>();
    }

    public static BackendBrpcService getInstance(BrpcConfig config) {
        if (INSTANCE == null) {
            synchronized (BackendBrpcService.class) {
                if (INSTANCE == null) {
                    INSTANCE = new BackendBrpcService(config);
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    protected void init() {
        LOG.info("Init brpc client manager");
    }

    @Override
    protected void reset() {
        endpointMap.values().forEach(endpoint -> endpoint.client.stop());
        endpointMap.clear();
        LOG.info("Reset brpc client manager");
    }

    public Future<PStreamLoadResponse> streamLoad(WorkerAddress backend, PStreamLoadRequest request, RpcCallback<PStreamLoadResponse> callback) {
        PBrpcServiceAsync service = getBackendService(backend);
        return service.streamLoad(request, callback);
    }

    private PBrpcServiceAsync getBackendService(WorkerAddress address) {
        return endpointMap.computeIfAbsent(address, this::createEndpoint).service;
    }

    private BrpcEndpoint createEndpoint(WorkerAddress address) {
        try {
            long start = System.currentTimeMillis();
            Endpoint endpoint = new Endpoint(address.getHost(), Integer.parseInt(address.getPort()));
            RpcClient rpcClient = new RpcClient(endpoint, config.clientOptions);
            long proxyStart = System.currentTimeMillis();
            PBrpcServiceAsync service = BrpcProxy.getProxy(rpcClient, PBrpcServiceAsync.class);
            LOG.info("Create brpc client, create cost: {} ms, proxy cost: {} ms, {}",
                    proxyStart - start, System.currentTimeMillis() - proxyStart, address);
            return new BrpcEndpoint(address, rpcClient, service);
        } catch (Exception e) {
            LOG.error("Failed to create brpc client, {}", address, e);
            throw e;
        }
    }

    private static class BrpcEndpoint {
        WorkerAddress address;
        RpcClient client;
        PBrpcServiceAsync service;

        public BrpcEndpoint(WorkerAddress address, RpcClient client, PBrpcServiceAsync service) {
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

    private static void runOnClient() {
        RpcClientOptions clientOptions = new RpcClientOptions();
        clientOptions.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        clientOptions.setConnectTimeoutMillis(1000);
        clientOptions.setReadTimeoutMillis(60000);
        clientOptions.setWriteTimeoutMillis(1000);
        clientOptions.setChannelType(ChannelType.POOLED_CONNECTION);
        clientOptions.setMaxTotalConnections(5);
        clientOptions.setMinIdleConnections(2);
        clientOptions.setMaxTryTimes(3);
        clientOptions.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
        clientOptions.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
        clientOptions.setIoThreadNum(Runtime.getRuntime().availableProcessors());
        clientOptions.setWorkThreadNum(Runtime.getRuntime().availableProcessors());
        BackendBrpcService.BrpcConfig brpcConfig = new BackendBrpcService.BrpcConfig(clientOptions);
        BackendBrpcService service = new BackendBrpcService(brpcConfig);
        try {
            service.takeRef();
            WorkerAddress address = new WorkerAddress("127.0.0.1", "11914");
            PStreamLoadRequest request = new PStreamLoadRequest();
            request.setDb("test");
            request.setTable("tbl");
            request.setUser("root");
            request.setPasswd("");
            List<PStringPair> parameters = new ArrayList<>();
            parameters.add(PStringPair.of("label", "test_abcd"));
            parameters.add(PStringPair.of("enable_merge_commit", "true"));
            parameters.add(PStringPair.of("merge_commit_interval_ms", "1000"));
            parameters.add(PStringPair.of("merge_commit_async", "false"));
            parameters.add(PStringPair.of("merge_commit_parallel", "4"));
            parameters.add(PStringPair.of("format", "json"));
            parameters.add(PStringPair.of("timeout", "60"));
            request.setParameters(parameters);
            byte[] data = "{\"c0\":9,\"c1\":\"12\"}".getBytes();
            RpcContext.getContext().setRequestBinaryAttachment(data);
            Future<PStreamLoadResponse> responseFuture = service.streamLoad(address, request, new TestRpcCallback());
            responseFuture.get();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            service.releaseRef();
        }
    }

    public static void main(String[] args) throws Exception {
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            threads.add(new Thread(BackendBrpcService::runOnClient));
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
    }

    private static class TestRpcCallback implements RpcCallback<PStreamLoadResponse> {

        @Override
        public void success(PStreamLoadResponse response) {
            System.out.println("Success to receive response");
        }

        @Override
        public void fail(Throwable throwable) {
            System.out.println("Failed to receive response");
            throwable.printStackTrace();
        }
    }
}
