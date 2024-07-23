package com.starrocks.data.load.stream.groupcommit;

import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import lombok.Getter;
import lombok.Setter;

@ProtobufClass
@Setter
@Getter
public class PGroupCommitLoadResponse {

    private long txnId;
    private String label;
    private String host;
    private String fragmentId;
    private long leftTimeMs;
    private String status;
    private String message;
    private long networkCostMs;
    private long loadCostMs;
    private long copyDataMs;
    private long groupCommitMs;
    private long pendingMs;
    private long waitPlanMs;
    private long appendMs;
    private long requestPlanNum;
    private long finishTs;
}
