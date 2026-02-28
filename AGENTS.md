# AGENTS.md

## Cursor Cloud specific instructions

This is a Java/Maven project: **StarRocks Connector for Apache Flink**. It consists of two sub-projects plus the main connector.

### Project structure

| Module | Path | Description |
|---|---|---|
| `starrocks-stream-load-sdk` | `starrocks-stream-load-sdk/` | Internal Stream Load SDK; must be built/installed first |
| `flink-connector-starrocks` | `.` (root) | Main Flink connector |
| `starrocks-thrift-sdk` | `starrocks-thrift-sdk/` | Pre-built Thrift SDK (no build needed) |

### Prerequisites

- **JDK 8** (`JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64`). The project targets source/target 1.8.
- **Maven** (system-installed, no wrapper). The CI uses `mvn -B -ntp`.
- **Docker** needed for integration tests (Testcontainers starts `starrocks/allin1-ubuntu:3.5.5`).

### Build order

1. `cd starrocks-stream-load-sdk && mvn -B -ntp clean install -DskipTests` — installs the SDK JAR to local Maven repo.
2. `cd /workspace && ./build.sh 1.20` — builds the connector for Flink 1.20 (default). Supported versions: 1.15–1.20.

### Lint

Checkstyle runs during the Maven `validate` phase automatically. To run standalone:
```
mvn -B -ntp checkstyle:check
```
Run in both `starrocks-stream-load-sdk/` and the root.

### Tests

- **Unit tests (no Docker):** `mvn -B -ntp test -Dtest='!*IT*,!*ITTest*'` in root, or `mvn -B -ntp test` in `starrocks-stream-load-sdk/`.
- **Integration tests (Docker required):** `mvn -B -ntp test -Dtest='com.starrocks.connector.flink.it.sink.StarRocksSinkITTest'` — Testcontainers auto-starts a StarRocks all-in-one container. Docker daemon must be running (`sudo dockerd`).
- The `@Ignore`-annotated `StarRocksITTest` in `it.container` package requires the `StarRocksCluster` multi-container setup and is not run by default.

### Non-obvious gotchas

- **JDK version:** The VM ships with JDK 21 as default; you must ensure `JAVA_HOME` points to JDK 8 and that `java -version` reports 1.8 before building. JDK 8 is set via `update-alternatives`.
- **Docker in Cloud VM:** Docker runs in nested-container mode with `fuse-overlayfs` storage driver and `iptables-legacy`. Start the daemon with `sudo dockerd &>/tmp/dockerd.log &` and wait a few seconds before using it. Grant non-root access with `sudo chmod 666 /var/run/docker.sock`.
- **Build flags for Flink version:** When running Maven commands directly (not via `build.sh`), pass `-Dflink.minor.version=1.20 -Dflink.version=1.20.0 -Dkafka.connector.version=3.4.0-1.20`. See `common.sh` for the full mapping.
- **Surefire + JUnit 4 Parameterized:** The surefire plugin 3.2.5 auto-detects JUnit Platform. Method-level test filters (e.g., `-Dtest='Class#method'`) may not match JUnit 4 `@Parameterized` tests. Use class-level filters instead.
- **Integration test duration:** A single IT test class (e.g., `StarRocksSinkITTest`) takes ~3 minutes including StarRocks container startup.
