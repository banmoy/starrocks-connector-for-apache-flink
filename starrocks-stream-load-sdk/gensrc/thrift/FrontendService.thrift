namespace java com.starrocks.thrift

struct TGetLabelStateRequest {
    1: optional list<string> dbs;
    2: optional list<string> labels;
}

struct TGetLabelStateResponse {
    1: optional list<string> status;
}

service TFrontendService {
    TGetLabelStateResponse getLabelState(1: TGetLabelStateRequest request)
}