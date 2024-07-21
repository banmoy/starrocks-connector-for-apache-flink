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
}
