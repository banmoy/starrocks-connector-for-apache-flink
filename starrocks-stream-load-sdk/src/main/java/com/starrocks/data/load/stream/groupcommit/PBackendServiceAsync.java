package com.starrocks.data.load.stream.groupcommit;

import com.baidu.brpc.client.RpcCallback;

import java.util.concurrent.Future;

public interface PBackendServiceAsync extends PBackendService {
    Future<PGroupCommitLoadResponse> groupCommitLoad(PGroupCommitLoadRequest request, RpcCallback<PGroupCommitLoadResponse> callback);
}
