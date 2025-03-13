package com.starrocks.data.load.stream.mergecommit;

public class EmptyMetricListener implements MetricListener {

    @Override
    public void onWrite(int numRows, int dataSize) {

    }

    @Override
    public void onCacheChange(long maxCacheBytes, long currentCacheBytes) {

    }

    @Override
    public void onCacheFull(long blockTimeMs) {

    }

    @Override
    public void onFlush(int numTables, long flushTimeMs) {

    }

    @Override
    public void onLoadStart(long dataSize, int numRows) {

    }

    @Override
    public void onLoadFailure(long dataSize, int numRows, int numRetry, long totalTimeMs) {

    }

    @Override
    public void onLoadSuccess(long dataSize, int numRows, int numRetry, long totalTimeMs, long serverTimeMs) {

    }
}
