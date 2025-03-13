package com.starrocks.data.load.stream.mergecommit;

public interface MetricListener {

    void onWrite(int numRows, int dataSize);
    void onCacheChange(long maxCacheBytes, long currentCacheBytes);
    void onCacheFull(long blockTimeMs);
    void onFlush(int numTables, long flushTimeMs);

    void onLoadStart(long dataSize, int numRows);
    void onLoadFailure(long dataSize, int numRows, int numRetry, long totalTimeMs);
    void onLoadSuccess(long dataSize, int numRows, int numRetry, long totalTimeMs, long serverTimeMs);
}
