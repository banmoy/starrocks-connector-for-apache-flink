package com.starrocks.data.load.stream.mergecommit.be;

import com.baidu.brpc.client.RpcCallback;

import java.util.concurrent.Future;

public interface PBrpcServiceAsync extends PBrpcService {
    Future<PStreamLoadResponse> streamLoad(PStreamLoadRequest request, RpcCallback<PStreamLoadResponse> callback);
}
