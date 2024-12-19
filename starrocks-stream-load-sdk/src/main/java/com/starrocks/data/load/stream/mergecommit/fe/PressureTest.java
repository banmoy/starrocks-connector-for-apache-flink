package com.starrocks.data.load.stream.mergecommit.fe;

import com.starrocks.data.load.stream.mergecommit.TableId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PressureTest {

    private static final boolean DEBUG = false;

    // PressureTest root "" http://127.0.0.1:8030 5 3 100 100 60000
    public static void main(String[] args) throws Exception {
        if (DEBUG) {
            args = new String[]{"root", "", "http://127.0.0.1:8030", "5", "3", "20", "100", "10000"};
        }
        String user = args[0];
        String password = args[1];
        String loadUrls = args[2];
        int numClients = Integer.parseInt(args[3]);
        int numConnectionsPerClient = Integer.parseInt(args[4]);
        int numTables = Integer.parseInt(args[5]);
        int intervalMs = Integer.parseInt(args[6]);
        int runTimeMs = Integer.parseInt(args[7]);

        DefaultFeHttpService.Config httpConfig = new DefaultFeHttpService.Config();
        httpConfig.username = user;
        httpConfig.password = password;
        httpConfig.candidateHosts = Arrays.asList(loadUrls.split(";"));

        NodesStateService.Config nodesConfig = new NodesStateService.Config();
        nodesConfig.updateIntervalMs = 2000;

        FeMetaService.Config feConfig = new FeMetaService.Config();
        feConfig.httpServiceConfig = httpConfig;
        feConfig.nodesStateServiceConfig = nodesConfig;
        feConfig.numExecutors = numConnectionsPerClient;

        List<Client> clients = new ArrayList<>();
        for (int i = 0; i < numClients; i++) {
            Client client = new Client(i, feConfig, numTables, intervalMs);
            clients.add(client);
            client.start();
        }
        try {
            Thread.sleep(runTimeMs);
        } catch (Exception e) {
            // ignore
        }
        for (Client client : clients) {
            client.stop();
        }

        AtomicLong count = new AtomicLong(0);
        AtomicLong maxLatency = new AtomicLong(0);
        AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);
        AtomicLong totalLatency = new AtomicLong(0);
        System.out.println("======================== Summary ========================");
        for (Client client : clients) {
            count.addAndGet(client.count.get());
            totalLatency.addAndGet(client.totalLatency.get());
            maxLatency.set(Math.max(maxLatency.get(), client.maxLatency.get()));
            minLatency.set(Math.min(minLatency.get(), client.minLatency.get()));
            client.print();
        }
        System.out.printf("Total: count=%s, avg=%s, max=%s, min=%s\n",
                count.get(), count.get() == 0 ? 0 : totalLatency.get() / count.get(), maxLatency.get(),
                minLatency.get() == Long.MAX_VALUE ? 0 : minLatency.get());
    }

    private static class Client {

        private final int id;
        private final FeMetaService service;
        private final int numTables;
        private final int intervalMs;
        private final ScheduledExecutorService executorService;
        private final AtomicBoolean stopped = new AtomicBoolean(false);
        private final AtomicLong loop = new AtomicLong(0);
        private final AtomicLong warmupCount = new AtomicLong(0);
        private final AtomicLong count = new AtomicLong(0);
        private final AtomicLong maxLatency = new AtomicLong(0);
        private final AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong totalLatency = new AtomicLong(0);

        public Client(int id, FeMetaService.Config feConfig, int numTables, int intervalMs) {
            this.id = id;
            this.service = new FeMetaService(feConfig);
            this.numTables = numTables;
            this.intervalMs = intervalMs;
            int processors = 300;
            this.executorService = Executors.newScheduledThreadPool(
                    processors,
                    r -> {
                        Thread thread = new Thread(null, r, "client-" + id);
                        thread.setDaemon(true);
                        return thread;
                    }
            );
        }

        public void start() throws Exception {
            service.takeRef();
            executorService.schedule(this::run, 0, TimeUnit.MILLISECONDS);
        }

        public void stop() throws Exception {
            if (!stopped.compareAndSet(false, true)) {
                return;
            }
            executorService.shutdownNow();
        }

        public void print() {
            System.out.printf("Client %s: count=%s, avg=%s, max=%s, min=%s\n",
                    id, count.get(), count.get() == 0 ? 0 : totalLatency.get() / count.get(), maxLatency.get(),
                    minLatency.get() == Long.MAX_VALUE ? 0 : minLatency.get());
        }

        private void run() {
            if (stopped.get()) {
                return;
            }
            long loop = this.loop.incrementAndGet();
            LabelStateService labelStateService = service.getLabelStateService().get();
            for (int i = 0; i < numTables; i++) {
                TableId tableId = TableId.of("eid" + i, "tbl");
                String label = String.format("label-%s-%s", i, loop);
                labelStateService.getFinalStatus(tableId, label, 0, 100, 600000)
                        .whenCompleteAsync(this::complete, executorService);
            }
            executorService.schedule(this::run, intervalMs, TimeUnit.MILLISECONDS);
        }

        private void complete(LabelStateService.LabelMeta labelMeta, Throwable throwable) {
            if (throwable != null) {
                System.out.println("Error: " + throwable.getMessage());
                return;
            }
            if (warmupCount.incrementAndGet() < 100) {
                return;
            }
            long latency = labelMeta.getLatencyMs();
            count.incrementAndGet();
            totalLatency.addAndGet(latency);
            while (true) {
                long currentMax = maxLatency.get();
                if (latency <= currentMax) {
                    break;
                }
                if (maxLatency.compareAndSet(currentMax, latency)) {
                    break;
                }
            }
            while (true) {
                long currentMin = minLatency.get();
                if (latency >= currentMin) {
                    break;
                }
                if (minLatency.compareAndSet(currentMin, latency)) {
                    break;
                }
            }
            if (count.get() % 5000 == 0) {
                print();
            }
        }
    }
}
