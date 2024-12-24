package com.starrocks.data.load.stream.mergecommit.be;

import com.baidu.brpc.protocol.BrpcMeta;

public interface PBrpcService {

    @BrpcMeta(serviceName = "PInternalService", methodName = "stream_load")
    PStreamLoadResponse streamLoad(PStreamLoadRequest request);
}
