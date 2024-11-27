package com.starrocks.data.load.stream.mergecommit;

import com.starrocks.data.load.stream.Chunk;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.mergecommit.be.PStreamLoadResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadRequest {

    private final Table table;
    private final Chunk chunk;
    private final AtomicInteger numRetries;
    private final List<Throwable> throwables;
    private String label;
    private StreamLoadResponse response;
    private CompletableFuture<?> future;

    public WorkerAddress workerAddress;
    public final long createTimeMs;
    // MergeCommitLoader#sendBrpc
    public long executeTimeMs;
    public long compressTimeMs;
    public long callRpcTimeMs;
    public long receiveResponseTimeMs;
    public long labelFinalTimeMs;

    public long rawSize;
    public long compressSize;
    PStreamLoadResponse loadResponse;

    public LoadRequest(Table table, Chunk chunk) {
        this.table = table;
        this.chunk = chunk;
        this.numRetries = new AtomicInteger(0);
        this.throwables = new ArrayList<>();
        this.createTimeMs = System.currentTimeMillis();
    }

    public void incRetries() {
        numRetries.getAndIncrement();
    }

    public int getRetries() {
        return numRetries.get();
    }

    public Table getTable() {
        return table;
    }

    public Chunk getChunk() {
        return chunk;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    public void setResponse(StreamLoadResponse response) {
        this.response = response;
    }

    public StreamLoadResponse getResponse() {
        return response;
    }

    public void addThrowable(Throwable throwable) {
        this.throwables.add(throwable);
    }

    public Throwable getFirstThrowable() {
        return throwables.isEmpty() ? null : throwables.get(0);
    }

    public Throwable getLastThrowable() {
        return throwables.isEmpty() ? null : throwables.get(throwables.size() - 1);
    }

    public List<Throwable> getThrowables() {
        return throwables;
    }

    public void setFuture(CompletableFuture<?> future) {
        this.future = future;
    }

    public CompletableFuture<?> getFuture() {
        return future;
    }

    public void reset() {
        label = null;
        response = null;
        future = null;
    }
}
