package com.starrocks.data.load.stream.mergecommit;

import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.RpcCallback;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.channel.ChannelType;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.data.load.stream.StreamLoadConstants;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.TransactionStatus;
import com.starrocks.data.load.stream.compress.NoCompressCodec;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.mergecommit.be.BackendBrpcService;
import com.starrocks.data.load.stream.mergecommit.be.PStreamLoadRequest;
import com.starrocks.data.load.stream.mergecommit.be.PStreamLoadResponse;
import com.starrocks.data.load.stream.mergecommit.be.PStringPair;
import com.starrocks.data.load.stream.mergecommit.fe.DefaultFeHttpService;
import com.starrocks.data.load.stream.mergecommit.fe.FeMetaService;
import com.starrocks.data.load.stream.mergecommit.fe.LabelStateService;
import com.starrocks.data.load.stream.mergecommit.fe.NodesStateService;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MergeCommitLoader implements StreamLoader, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(MergeCommitLoader.class);

    private StreamLoadProperties properties;
    private ObjectMapper objectMapper;
    private BackendBrpcService brpcService = null;
    private FeMetaService feMetaService = null;
    private ScheduledExecutorService executorService;

    public MergeCommitLoader() {
    }

    @Override
    public void start(StreamLoadProperties properties, StreamLoadManager manager) {
        try {
            this.properties = properties;
            this.objectMapper = new ObjectMapper();
            // StreamLoadResponseBody does not contain all fields returned by StarRocks
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            // filed names in StreamLoadResponseBody are case-insensitive
            objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

            RpcClientOptions clientOptions = new RpcClientOptions();
            clientOptions.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
            clientOptions.setConnectTimeoutMillis(600000);
            clientOptions.setReadTimeoutMillis(600000);
            clientOptions.setWriteTimeoutMillis(600000);
            clientOptions.setChannelType(ChannelType.POOLED_CONNECTION);
            clientOptions.setMaxTotalConnections(properties.getBrpcMaxConnections());
            clientOptions.setMinIdleConnections(properties.getBrpcMinConnections());
            clientOptions.setMaxTryTimes(3);
            clientOptions.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
            clientOptions.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
            int nproc = Runtime.getRuntime().availableProcessors();
            clientOptions.setIoThreadNum(properties.getBrpcIoThreadNum() > 0 ? properties.getBrpcIoThreadNum() : nproc);
            clientOptions.setWorkThreadNum(properties.getBrpcWorkerThreadNum() > 0 ? properties.getBrpcWorkerThreadNum() : nproc);
            BackendBrpcService.BrpcConfig brpcConfig = new BackendBrpcService.BrpcConfig(clientOptions);
            BackendBrpcService service = BackendBrpcService.getInstance(brpcConfig);
            service.takeRef();
            brpcService = service;

            DefaultFeHttpService.Config httpConfig = new DefaultFeHttpService.Config();
            httpConfig.username = properties.getUsername();
            httpConfig.password = properties.getPassword();
            httpConfig.candidateHosts = Arrays.asList(properties.getLoadUrls());
            NodesStateService.Config nodesConfig = new NodesStateService.Config();
            nodesConfig.updateIntervalMs = 2000;
            FeMetaService.Config config = new FeMetaService.Config();
            config.httpServiceConfig = httpConfig;
            config.nodesStateServiceConfig = nodesConfig;
            config.numExecutors = properties.getHttpMaxConnections();
            FeMetaService service1 = FeMetaService.getInstance(config);
            service1.takeRef();
            feMetaService = service1;

            this.executorService = new ScheduledThreadPoolExecutor(
                    properties.getIoThreadCount(),
                    r -> {
                        Thread thread = new Thread(null, r, "merge-commit-load-" + UUID.randomUUID());
                        thread.setDaemon(true);
                        thread.setUncaughtExceptionHandler((t, e) -> {
                            LOG.error("Stream loader " + Thread.currentThread().getName() + " error", e);
                            manager.callback(e);
                        });
                        return thread;
                    });
        } catch (Throwable e) {
            if (feMetaService != null) {
                feMetaService.releaseRef();
                feMetaService = null;
            }
            if (brpcService != null) {
                brpcService.releaseRef();
                brpcService = null;
            }
            throw new RuntimeException("Failed to start merge commit loader", e);
        }
    }

    @Override
    public void close() {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        if (feMetaService != null) {
            feMetaService.releaseRef();
            feMetaService = null;
        }
        if (brpcService != null) {
            brpcService.releaseRef();
            brpcService = null;
        }
    }

    public ScheduledFuture<?> scheduleFlush(Table table, long chunkId, int delayMs) {
        return executorService.schedule(() -> table.checkFlushInterval(chunkId), delayMs, TimeUnit.MILLISECONDS);
    }

    public void sendLoad(LoadRequest request, int delayMs) {
        executorService.schedule(() -> sendBrpc(request), delayMs, TimeUnit.MILLISECONDS);
    }

    private void sendBrpc(LoadRequest loadRequest) {
        loadRequest.executeTimeMs = System.currentTimeMillis();
        Table table = loadRequest.getTable();
        String database = table.getDatabase();
        String tableName = table.getTable();
        TableId tableId = TableId.of(database, tableName);
        try {
            byte[] data = ChunkCompressUtil.compress(loadRequest.getChunk(),
                    table.getCompressionCodec().orElseGet(NoCompressCodec::new));
            loadRequest.rawSize = loadRequest.getChunk().chunkBytes();
            loadRequest.compressSize = data.length;
            loadRequest.compressTimeMs = System.currentTimeMillis();
            Future<WorkerAddress> brpcAddressFuture = feMetaService.getNodesStateService()
                    .get().getBrpcAddress(tableId, table.getLoadParameters());
            WorkerAddress workerAddress = brpcAddressFuture.get();
            loadRequest.getBrpcAddrTimeMs = System.currentTimeMillis();
            String userLabel = UUID.randomUUID().toString();
            loadRequest.setLabel(userLabel);
            PStreamLoadRequest request = new PStreamLoadRequest();
            request.setDb(database);
            request.setTable(tableName);
            request.setUser(properties.getUsername());
            request.setPasswd(properties.getPassword());
            List<PStringPair> parameters = new ArrayList<>();
            table.getLoadParameters().forEach((k, v) -> parameters.add(PStringPair.of(k, v)));
            parameters.add(PStringPair.of("label", userLabel));
            request.setParameters(parameters);
            RpcContext.getContext().setRequestBinaryAttachment(data);
            LoadRpcCallback callback = new LoadRpcCallback(loadRequest);
            loadRequest.workerAddress = workerAddress;
            brpcService.streamLoad(workerAddress, request, callback);
            loadRequest.callRpcTimeMs = System.currentTimeMillis();
            LOG.info(
                    "Send merge commit load request, db: {}, table: {}, user label: {}, chunkId: {}, worker: {}",
                    database, tableName, userLabel, loadRequest.getChunk().getChunkId(), workerAddress.getHost());
        } catch (Throwable e) {
            LOG.error("Failed to send load brpc, db: {}, table: {}, chunkId: {}",
                    database, tableName, loadRequest.getChunk().getChunkId(), e);
            table.loadFinish(loadRequest, e);
        }
    }

    private void waitLabelAsync(LoadRequest request) {
        Table table = request.getTable();
        long leftTimeMs = request.getResponse().getBody().getLeftTimeMs() == null ? -1 :
            request.getResponse().getBody().getLeftTimeMs();
        CompletableFuture<LabelStateService.LabelMeta> future =
                feMetaService.getLabelStateService()
                        .get().getFinalStatus(
                                TableId.of(table.getDatabase(), table.getTable()),
                                request.getResponse().getBody().getLabel(),
                                (int) leftTimeMs,
                                properties.getCheckLabelIntervalMs(),
                                properties.getCheckLabelTimeoutMs())
                        .whenCompleteAsync(
                                (labelMeta, throwable)
                                        -> dealLabelStatus(request, labelMeta, throwable),
                                executorService);
        request.setFuture(future);
    }

    private void dealLabelStatus(LoadRequest request,
                                 LabelStateService.LabelMeta labelMeta,
                                 Throwable throwable) {
        TransactionStatus status = labelMeta.transactionStatus;
        request.labelFinalTimeMs = System.currentTimeMillis();
        if (throwable != null) {
            request.getTable().loadFinish(request, throwable);
            return;
        }

        if (status != TransactionStatus.VISIBLE &&
                status != TransactionStatus.COMMITTED) {
            request.getTable().loadFinish(
                    request,
                    new RuntimeException(String.format(
                            "Label %s does not in final status, current status: %s",
                            request.getResponse().getBody().getLabel(), status)));
        } else {
            request.getTable().loadFinish(request, null);
            logRequestTrace(request, labelMeta);
        }
    }

    private static void logRequestTrace(LoadRequest request,
                                        LabelStateService.LabelMeta labelMeta) {
        LOG.info(
                "Cost trace, db: {}, table: {}, chunkId: {}, userLabel: {}, worker: {}, raw/compress: {}/{}, "
                        +
                        "total: {}, pending: {}, compress: {}, getBrpcAddr: {}, callRpc: {}, server: {}, "
                        +
                        "waitLabel:  {}",
                request.getTable().getDatabase(), request.getTable().getTable(),
                request.getChunk().getChunkId(), request.getLabel(), request.workerAddress.getHost(),
                request.rawSize, request.compressSize, request.labelFinalTimeMs - request.createTimeMs,
                request.executeTimeMs - request.createTimeMs,
                request.compressTimeMs - request.executeTimeMs,
                request.getBrpcAddrTimeMs - request.compressTimeMs,
                request.callRpcTimeMs - request.getBrpcAddrTimeMs,
                request.receiveResponseTimeMs - request.callRpcTimeMs,
                request.labelFinalTimeMs - request.receiveResponseTimeMs);
    }

    private class LoadRpcCallback implements RpcCallback<PStreamLoadResponse> {

        private final LoadRequest request;

        public LoadRpcCallback(LoadRequest request) { this.request = request; }

        @Override
        public void success(PStreamLoadResponse response) {
            request.receiveResponseTimeMs = System.currentTimeMillis();
            String db = request.getTable().getDatabase();
            String table = request.getTable().getTable();

            LOG.info(
                    "Receive merge commit load response, db: {}, table: {}, user label: {}, chunkId: {}, ",
                    db, table, request.getLabel(), request.getChunk().getChunkId());

            request.loadResponse = response;
            String jsonResult = response.getJson_result();
            StreamLoadResponse.StreamLoadResponseBody streamLoadBody;
            try {
                streamLoadBody = objectMapper.readValue(jsonResult, StreamLoadResponse.StreamLoadResponseBody.class);
            } catch (Exception e) {
                String errorMsg = "Fail to parse response, json response: " + jsonResult;
                Throwable exception = new StreamLoadFailException(errorMsg, e);
                LOG.error("Fail to parse response, db: {}, table: {}, worker: {}, json result: {}", db, table,
                        request.workerAddress.getHost(), jsonResult, e);
                request.getTable().loadFinish(request, exception);
                return;
            }
            StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
            streamLoadResponse.setBody(streamLoadBody);
            String status = streamLoadBody.getStatus();
            if (StreamLoadConstants.RESULT_STATUS_SUCCESS.equals(status)) {
                request.setResponse(streamLoadResponse);
                waitLabelAsync(request);
            } else {
                String errorMsg = String.format(
                        "Stream load failed because of error, db: %s, table: %s, user label: %s, worker: %s, "
                                + "response: %s",
                        db, table, request.getLabel(), request.workerAddress.getHost(), response);
                Throwable throwable =
                        new StreamLoadFailException(errorMsg, streamLoadBody);
                request.getTable().loadFinish(request, throwable);
            }
        }

        @Override
        public void fail(Throwable throwable) {
            String db = request.getTable().getDatabase();
            String table = request.getTable().getTable();
            LOG.error("Send merge commit load failure, db: {}, table: {}, user label: {}, chunkId: {}, worker: {}",
                    db, table, request.getLabel(), request.getChunk().getChunkId(), request.workerAddress.getHost(), throwable);
            String errorMsg = String.format("Send merge commit load failure, db: %s, table: %s, user label: %s, worker: %s",
                    db, table, request.getLabel(), request.workerAddress.getHost());
            Throwable exception = new StreamLoadFailException(errorMsg, throwable);
            request.getTable().loadFinish(request, exception);
        }
    }

    @Override
    public boolean begin(TableRegion region) {
        return false;
    }

    @Override
    public Future<StreamLoadResponse> send(TableRegion region) {
        return null;
    }

    @Override
    public Future<StreamLoadResponse> send(TableRegion region, int delayMs) {
        return null;
    }

    @Override
    public boolean prepare(StreamLoadSnapshot.Transaction transaction) {
        return false;
    }

    @Override
    public boolean commit(StreamLoadSnapshot.Transaction transaction) {
        return false;
    }

    @Override
    public boolean rollback(StreamLoadSnapshot.Transaction transaction) {
        return false;
    }

    @Override
    public boolean prepare(StreamLoadSnapshot snapshot) {
        return false;
    }

    @Override
    public boolean commit(StreamLoadSnapshot snapshot) {
        return false;
    }

    @Override
    public boolean rollback(StreamLoadSnapshot snapshot) {
        return false;
    }

    @Override
    public TransactionStatus getLoadStatus(String db, String table, String label) throws Exception {
        return null;
    }
}
