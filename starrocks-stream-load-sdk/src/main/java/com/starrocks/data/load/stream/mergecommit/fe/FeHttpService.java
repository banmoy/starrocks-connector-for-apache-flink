package com.starrocks.data.load.stream.mergecommit.fe;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;

public interface FeHttpService {

    Pair<Integer, String> getNodes(String db, String table, Map<String, String> headers) throws Exception;

    Pair<Integer, String> getLabelState(String db, String label) throws Exception;
}
