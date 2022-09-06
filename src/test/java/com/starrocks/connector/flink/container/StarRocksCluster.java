/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.container;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.io.BaseEncoding;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

public class StarRocksCluster implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCluster.class);

    private final static Duration DEFAULT_CONTAINER_START_TIMEOUT = Duration.ofMinutes(5);
    private final static Duration DEFAULT_ADD_COMPONENTS_TIMEOUT = Duration.ofSeconds(60);

    private final int numFeFollowers;
    private final int numFeObservers;
    private final int numBes;

    private final String starRocksVersion;

    private final Network network = Network.newNetwork();

    private final List<StarRocksFEContainer> feFollowers;
    private final List<StarRocksFEContainer> feObservers;
    private final List<StarRocksBEContainer> bes;

    public StarRocksCluster(int numFeFollowers, int numFeObservers, int numBes) {
        this(numFeFollowers, numFeObservers, numBes, StarRocksImageBuilder.DEFAULT_STARROCKS_VERSION);
    }

    public StarRocksCluster(int numFeFollowers, int numFeObservers, int numBes, String starRocksVersion) {
        // TODO currently only support one FE
        Preconditions.checkArgument(numFeFollowers == 1, "There must be at least one FE follower.");
        Preconditions.checkArgument(numFeObservers == 0, "Number of FE observers must be non-negative.");
        Preconditions.checkArgument(numBes > 0, "There must be at least one BE.");

        this.numFeFollowers = numFeFollowers;
        this.numFeObservers = numFeObservers;
        this.numBes = numBes;
        this.starRocksVersion = starRocksVersion;
        this.feFollowers = new ArrayList<>();
        this.feObservers = new ArrayList<>();
        this.bes = new ArrayList<>();
    }

    public String getQueryUrls() {
        return getFeUrls(StarRocksFEContainer.QUERY_PORT);
    }

    public String getHttpUrls() {
        return getFeUrls(StarRocksFEContainer.HTTP_PORT);
    }

    private String getFeUrls(int port) {
        StringBuilder builder = new StringBuilder();
        for (StarRocksFEContainer container : feFollowers) {
            if (builder.length() > 1) {
                builder.append(",");
            }
            builder.append("127.0.0.1:");
            builder.append(container.getMappedPort(port));
        }

        for (StarRocksFEContainer container : feObservers) {
            builder.append(",127.0.0.1:");
            builder.append(container.getMappedPort(port));
        }

        return builder.toString();
    }

    public String getBeUrlMapping() {
        StringBuilder builder = new StringBuilder();
        for (StarRocksBEContainer container : bes) {
            if (builder.length() != 0) {
                builder.append(";");
            }
            builder.append("127.0.0.1:");
            builder.append(container.getMappedPort(StarRocksBEContainer.BE_PORT));
            builder.append(",");
            builder.append(container.getNetworkIp());
            builder.append(":");
            builder.append(StarRocksBEContainer.BE_PORT);
        }

        return builder.toString();
    }

    public void executeMysqlCommand(String cmd) {
        StarRocksFEContainer container = feFollowers.get(0);
        container.executeMysqlCmd(cmd);
    }

    public void start() throws Exception {
        LOG.info("Start to build the image for StarRocks {} ......", starRocksVersion);
        StarRocksImage starRocksImage = new StarRocksImageBuilder()
                                            .setStarRocksVersion(starRocksVersion)
                                            .setDeleteOnExit(false)
                                            .build();

        LOG.info("Start to build the cluster with {} FE followers, {} FE observers, and {} BEs......",
                numFeFollowers, numFeObservers, numBes);
        // 1. create FE follower, FE observer, and BE containers
        for (int i = 0; i < numFeFollowers; i++) {
            // select the first FE as the helper
            StarRocksFEContainer feHelper = i == 0 ? null : feFollowers.get(0);
            StarRocksFEContainer follower = createFEContainer(starRocksImage, feHelper, false, "fe-follower-" + i);
            feFollowers.add(follower);
        }

        for (int i = 0; i < numFeObservers; i++) {
            StarRocksFEContainer observer = createFEContainer(starRocksImage, feFollowers.get(0), true, "fe-observer-" + i);
            feObservers.add(observer);
        }

        for (int i = 0; i < numBes; i++) {
            StarRocksBEContainer be = createBEContainer(starRocksImage, "be-" + i);
            bes.add(be);
        }

        // 2. start other containers and wait until each can be reachable
        List<StarRocksContainer<?>> containers = new ArrayList<>();
        containers.addAll(feFollowers);
        containers.addAll(feObservers);
        containers.addAll(bes);
        containers.parallelStream().forEach(StarRocksContainer::start);
        containers.parallelStream().forEach(container -> container.waitUntilReachable(DEFAULT_CONTAINER_START_TIMEOUT));

        // 3. add all components to the cluster via the FE helper
        StarRocksFEContainer helper = feFollowers.get(0);
        feFollowers.subList(1, feFollowers.size()).forEach(helper::addFEFollower);
        feObservers.forEach(helper::addFEObserver);
        bes.forEach(helper::addBE);

        // 4. wait for the cluster is ready
        waitUntilFEReady(helper);
        waitUntilBEReady(helper);

        LOG.info("Successful to start the cluster");
    }

    /**
     * Restart all FEs and BEs, and return a future to indicate
     * whether the cluster is ready.
     */
    public CompletableFuture<Boolean> restartCluster() {
        throw new UnsupportedOperationException();
    }

    /**
     * Restart all FEs, and return a future to indicate whether
     * the FEs are restarted, and ready to serve requests.
     */
    public CompletableFuture<Boolean> restartAllFe() {
        throw new UnsupportedOperationException();
    }

    /**
     * Restart all BEs, and return a future to indicate whether
     * the BEs are restarted, and ready to serve requests.
     */
    public CompletableFuture<Boolean> restartAllBe() {
        throw new UnsupportedOperationException();
    }


    public void stop() {
        LOG.info("Stop the cluster");
        bes.parallelStream().forEach(StarRocksBEContainer::stop);
        feObservers.parallelStream().forEach(StarRocksFEContainer::stop);
        feFollowers.parallelStream().forEach(StarRocksFEContainer::stop);
    }

    @Override
    public void close() {
        LOG.info("Close the cluster");
        bes.parallelStream().forEach(StarRocksBEContainer::close);
        feObservers.parallelStream().forEach(StarRocksFEContainer::close);
        feFollowers.parallelStream().forEach(StarRocksFEContainer::close);
        network.close();
    }

    private StarRocksFEContainer createFEContainer(
            StarRocksImage starRocksImage, @Nullable StarRocksFEContainer helper, boolean isObserver, String id) {
        StarRocksFEContainer container = new StarRocksFEContainer(DockerImageName.parse(starRocksImage.getImageName()), id, isObserver);
        String scriptPath = new File(starRocksImage.getStarRocksHome(), "fe/bin/start_fe.sh").getAbsolutePath();
        String helperCmd = helper == null ? "" : String.format("--helper %s:%s", helper.getId(), StarRocksFEContainer.EDIT_LOG_PORT);

        container.withNetwork(network)
                .withNetworkAliases(id)
                .withExposedPorts(
                        StarRocksFEContainer.HTTP_PORT,
                        StarRocksFEContainer.RPC_PORT,
                        StarRocksFEContainer.QUERY_PORT,
                        StarRocksFEContainer.EDIT_LOG_PORT)
                .withEnv("JAVA_HOME", "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.342.b07-1.el7_9.x86_64")
                .withCommand("/bin/bash", "-c", scriptPath, helperCmd)
                .withLogConsumer(new Slf4jLogConsumer(StarRocksFEContainer.LOG).withPrefix(id));
        LOG.info("Create FE container {}", id);
        return container;
    }

    private StarRocksBEContainer createBEContainer(StarRocksImage starRocksImage, String id) {
        StarRocksBEContainer container = new StarRocksBEContainer(DockerImageName.parse(starRocksImage.getImageName()), id);
        String scriptPath = new File(starRocksImage.getStarRocksHome(), "be/bin/start_be.sh").getAbsolutePath();
        container.withNetwork(network)
                .withNetworkAliases(id)
                .withExposedPorts(
                        StarRocksBEContainer.BE_PORT,
                        StarRocksBEContainer.WEB_SERVER_PORT,
                        StarRocksBEContainer.HEARBEAT_SERVICE_PORT,
                        StarRocksBEContainer.BRPC_PORT)
                .withEnv("JAVA_HOME", "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.342.b07-1.el7_9.x86_64")
                .withCommand("/bin/bash", "-c", scriptPath)
                .withLogConsumer(new Slf4jLogConsumer(StarRocksBEContainer.LOG).withPrefix(id));
        LOG.info("Create BE container {}", id);
        return container;
    }

    private void waitUntilFEReady(StarRocksFEContainer helper) {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < DEFAULT_ADD_COMPONENTS_TIMEOUT.toMillis()) {
            final AtomicInteger numFes = new AtomicInteger(-1);
            try {
                numFes.set(-1);
                new HttpWaitStrategy()
                        .forPort(StarRocksFEContainer.HTTP_PORT)
                        .forPath("/api/show_meta_info?action=show_ha")
                        .withBasicCredentials("root", "")
                        .forStatusCode(200)
                        .forResponsePredicate(response -> {
                            JSONObject object = JSON.parseObject(response);
                            String electableNodes = object.getString("electable_nodes");
                            int numFollowers = electableNodes == null || electableNodes.isEmpty() ? 0 : electableNodes.split(",").length;
                            String observerNodes = object.getString("observer_nodes");
                            int numObservers = observerNodes == null || observerNodes.isEmpty() ? 0 : observerNodes.split(",").length;
                            numFes.set(numFollowers + numObservers);
                            return numFollowers + 1 == feFollowers.size() && numObservers == feObservers.size();
                        })
                        .withReadTimeout(DEFAULT_ADD_COMPONENTS_TIMEOUT)
                        .waitUntilReady(helper);
                    break;
            } catch (Exception e) {
                // throw exception if not receive normal response, otherwise retry
                if (numFes.get() == -1) {
                    String errMsg = "Failed to wait for FEs to be ready";
                    LOG.error("{}", errMsg, e);
                    throw new StarRocksContainerException(errMsg, e);
                }
            }

            try {
                Thread.sleep(10);
            } catch (Exception e) {
                break;
            }
        }
    }

    private void waitUntilBEReady(StarRocksFEContainer helper) {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < DEFAULT_ADD_COMPONENTS_TIMEOUT.toMillis()) {
            final AtomicInteger numBes = new AtomicInteger(-1);
            try {
                numBes.set(-1);
                new HttpWaitStrategy()
                        .forPort(StarRocksFEContainer.HTTP_PORT)
                        .forPath("/api/health")
                        .withBasicCredentials("root", "")
                        .forStatusCode(200)
                        .forResponsePredicate(response -> {
                            int num = JSON.parseObject(response).getInteger("online_backend_num");
                            numBes.set(num);
                            return num == bes.size();
                        })
                        .withReadTimeout(DEFAULT_ADD_COMPONENTS_TIMEOUT)
                        .waitUntilReady(helper);
                break;
            } catch (Exception e) {
                if (numBes.get() == -1) {
                    // throw exception if not receive normal response, otherwise retry
                    String errMsg = "Failed to wait for BEs to be ready";
                    LOG.error("{}", errMsg, e);
                    throw new StarRocksContainerException(errMsg, e);
                }
            }

            try {
                Thread.sleep(10);
            } catch (Exception e) {
                break;
            }
        }
    }

    private static String buildAuthString(String username, String password) {
        return "Basic" + BaseEncoding.base64().encode((username + ":" + password).getBytes());
    }

    private static String getResponseBody(HttpURLConnection connection) throws IOException {
        BufferedReader reader;
        if (200 <= connection.getResponseCode() && connection.getResponseCode() <= 299) {
            reader = new BufferedReader(new InputStreamReader((connection.getInputStream())));
        } else {
            reader = new BufferedReader(new InputStreamReader((connection.getErrorStream())));
        }

        StringBuilder builder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            builder.append(line);
        }
        return builder.toString();
    }

    private static void testHttp() throws Exception {
        final HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:33193/api/show_meta_info?action=show_ha").openConnection();
        connection.setRequestProperty("Authorization", buildAuthString("root", ""));
        connection.setUseCaches(false);
        connection.setRequestMethod("GET");
        connection.connect();

        String response = getResponseBody(connection);
        JSONObject object = JSON.parseObject(response);
        String electableNodes = object.getString("electable_nodes");
        int numFollowers = electableNodes == null || electableNodes.isEmpty() ? 0 : electableNodes.split(",").length;
        String observerNodes = object.getString("observer_nodes");
        int numObservers = observerNodes == null || observerNodes.isEmpty() ? 0 : observerNodes.split(",").length;
        System.out.println(response);
        System.out.println(numFollowers);
        System.out.println(numObservers);
    }

    public static void main(String[] args) throws Exception {
        int numFeFollowers = 1;
        int numFeObservers = 0 ;
        int numBes = 0;
        if (args.length > 0) {
            numFeFollowers = Integer.parseInt(args[0]);
        }

        if (args.length > 1) {
            numFeObservers = Integer.parseInt(args[1]);
        }

        if (args.length > 2) {
            numBes = Integer.parseInt(args[2]);
        }

        try (StarRocksCluster cluster =
                new StarRocksCluster(numFeFollowers, numFeObservers, numBes)) {
            cluster.start();
            System.out.println("JDBC URL: jdbc:mysql://" + cluster.getQueryUrls());
            System.out.println("Load URL: " + cluster.getHttpUrls());
            System.out.println("BE Mapping: " + cluster.getBeUrlMapping());

            String createDbCmd = "CREATE DATABASE " + "starrocks_connector_it";
            cluster.executeMysqlCommand(createDbCmd);

            String createTable =
                    "CREATE TABLE " + "starrocks_connector_it." + "sink_table" + " (" +
                            "name STRING," +
                            "score BIGINT," +
                            "t DATETIME," +
                            "a JSON," +
                            "e ARRAY<JSON>," +
                            "f ARRAY<STRING>," +
                            "g ARRAY<DECIMALV2(2,1)>," +
                            "h ARRAY<ARRAY<STRING>>," +
                            "i JSON," +
                            "j JSON," +
                            "k JSON," +
                            "d DATE" +
                            ") ENGINE = OLAP " +
                            "DUPLICATE KEY(name)" +
                            "DISTRIBUTED BY HASH (name) BUCKETS 8";
            cluster.executeMysqlCommand(createTable);

            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (Exception e) {
                // ignore
            }
            cluster.stop();
        } catch (Exception e) {
            LOG.error("Failed to run", e);
            throw e;
        }
    }
}
