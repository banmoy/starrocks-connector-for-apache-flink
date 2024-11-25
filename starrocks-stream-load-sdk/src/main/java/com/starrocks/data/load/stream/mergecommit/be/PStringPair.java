package com.starrocks.data.load.stream.mergecommit.be;

import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import lombok.Getter;
import lombok.Setter;

@ProtobufClass
@Setter
@Getter
public class PStringPair {

    private String key;
    private String val;

    public static PStringPair of(String key, String value) {
        PStringPair keyValue = new PStringPair();
        keyValue.setKey(key);
        keyValue.setVal(value);
        return keyValue;
    }

    @Override
    public String toString() {
        return "PStringPair{" +
                "key='" + key + '\'' +
                ", value='" + val + '\'' +
                '}';
    }
}
