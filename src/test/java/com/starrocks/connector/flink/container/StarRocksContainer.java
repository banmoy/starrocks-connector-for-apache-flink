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

import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.NetworkSettings;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;

/** Base class for StarRocks container. */
public abstract class StarRocksContainer<CONTAINER extends StarRocksContainer<CONTAINER>> extends GenericContainer<CONTAINER> {

    /**
     * Unique identifier of the container, and will be used as the
     * network alias via {@link #withNetworkAliases};
     */
    private final String id;

    public StarRocksContainer(DockerImageName dockerImageName, String id) {
        super(dockerImageName);
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public String getNetworkIp() {
        NetworkSettings settings = DockerClientFactory.instance().client()
                .inspectContainerCmd(getContainerId()).exec().getNetworkSettings();
        System.out.println("getIpAddress: " +  settings.getIpAddress());
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, ContainerNetwork> entry : settings.getNetworks().entrySet()) {
            if (sb.length() != 0) {
                sb.append(",");
            }
            sb.append(entry.getKey());
            sb.append(":");
            sb.append(entry.getValue().getIpAddress());
        }
        System.out.println(sb);
        return settings.getNetworks().values().iterator().next().getIpAddress();
    }

    /**
     * Wait until the process in container can be reachable.
     */
    public abstract void waitUntilReachable(Duration timeout);
}
