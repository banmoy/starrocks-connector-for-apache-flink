package com.starrocks.data.load.stream.groupcommit;

import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import lombok.Getter;
import lombok.Setter;

@ProtobufClass
@Setter
@Getter
public class PGroupCommitLoadRequest {

    private String db;
    private String table;
    private String userLabel;
    private long timeout;
    private long clientTimeMs;
    private byte[] data;
}
