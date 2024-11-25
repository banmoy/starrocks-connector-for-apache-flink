package com.starrocks.data.load.stream.mergecommit.be;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@ProtobufClass
@Setter
@Getter
public class PStreamLoadRequest {

    private String db;
    private String table;
    private String user;
    private String passwd;
    @Protobuf(fieldType= FieldType.OBJECT)
    private List<PStringPair> parameters;

    @Override
    public String toString() {
        return "PStreamLoadRequest{" +
                "db='" + db + '\'' +
                ", table='" + table + '\'' +
                ", user='" + user + '\'' +
                ", passwd='" + passwd + '\'' +
                ", parameters=" + parameters +
                '}';
    }
}
