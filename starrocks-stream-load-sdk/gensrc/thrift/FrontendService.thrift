namespace java com.starrocks.thrift

struct TGetLabelStateRequest {
    1: optional list<string> dbs;
    2: optional list<string> labels;
}

struct TGetLabelStateResponse {
    1: optional list<string> status;
}

struct TGetGroupCommitMetaRequest {
    1: optional list<string> dbs;
    2: optional list<string> tables;
}

struct TGetGroupCommitMetaResponse {
    1: optional list<string> be_http_metas;
    2: optional list<string> be_brpc_metas;
}

service TFrontendService {
    TGetLabelStateResponse getLabelState(1: TGetLabelStateRequest request)
    TGetGroupCommitMetaResponse getGroupCommitMeta(1: TGetGroupCommitMetaRequest request)
}