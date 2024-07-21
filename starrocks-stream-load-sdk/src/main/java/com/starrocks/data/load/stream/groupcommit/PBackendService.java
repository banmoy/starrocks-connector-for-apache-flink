package com.starrocks.data.load.stream.groupcommit;

import com.baidu.brpc.protocol.BrpcMeta;

public interface PBackendService {

    @BrpcMeta(serviceName = "PBackendService", methodName = "group_commit_load")
    PGroupCommitLoadResponse groupCommitLoad(PGroupCommitLoadRequest request);
}
