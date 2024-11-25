package com.starrocks.data.load.stream.mergecommit.be;

import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import lombok.Getter;
import lombok.Setter;

@ProtobufClass
@Setter
@Getter
public class PStreamLoadResponse {

    private String json_result;

    @Override
    public String toString() {
        return "PStreamLoadResponse{" +
                "json_result='" + json_result + '\'' +
                '}';
    }
}
