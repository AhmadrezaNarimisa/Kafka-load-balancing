# kafka-load-balancing

A Kotlin + Spring Boot showcase of **Kafka load balancing** at three layers:

| Layer | Technique | Where to watch |
|---|---|---|
| **Multi-cluster** | MirrorMaker 2 replication (A → B) + automatic produce failover | `/api/orders?cluster=auto`, Kafka UI replication lag |
| **Consumer group** | Partition-based balancing across instances; two independent groups | app logs (`ASSIGNED`/`REVOKED` events), Kafka UI consumers view |
| **Producer** | Sticky partitioning (keyless) vs custom `SegmentPartitioner` | HTTP responses: which partition each message lands on |

## Stack

- Kotlin 2.x / Spring Boot 4.1 / Gradle (kotlin DSL, wrapper included)
- Apache Kafka 3.9 (KRaft mode, no Zookeeper) — two clusters
- MirrorMaker 2 (`connect-mirror-maker.sh` + `infra/mm2.properties`)
- provectuslabs/kafka-ui to observe both clusters

## Quick start

```bash
# 1. infrastructure: 2 clusters + MirrorMaker 2 + Kafka UI
docker compose up -d

# 2. run the app (instance A)
./gradlew bootRun --args='--server.port=8080'

# or run several instances to see consumer group balancing:
INSTANCE_ID=instance-2 ./gradlew bootRun --args='--server.port=8081'
INSTANCE_ID=instance-3 ./gradlew bootRun --args='--server.port=8082'
```

Kafka UI: http://localhost:8090 (clusters `primary-A` and `secondary-B`).

## Demo scenarios

### 1. Consumer-group partitioning (load balancing across instances)

```bash
curl -X POST localhost:8080/api/orders -H 'Content-Type: application/json' \
  -d '{"product":"laptop","amount":1200,"key":"order-1"}'
curl -X POST localhost:8080/api/orders -H 'Content-Type: application/json' \
  -d '{"product":"phone","amount":800,"key":"order-2"}'
# ... repeat ~20 times with different keys
```

Each response shows the topic/partition/offset the message landed on.
Run multiple app instances (see Quick start) and watch lines like:

```
instance [instance-2] rebalance ASSIGNED: partitions orders-topic#0, orders-topic#1
instance [instance-3] rebalance ASSIGNED: partitions orders-topic#2, ...
```

Kafka distributes the 6 partitions among the running instances — that *is*
Kafka's load balancing inside a consumer group. Stop one instance and watch the
`REVOKED` + `ASSIGNED` events as partitions get reassigned.

Two groups (`billing`, `analytics`) each receive the full stream, independently
balanced. Consume from the secondary cluster by overriding `app.consume-from=secondary`.

### 2. Producer partitioning strategies

- `strategy=sticky` (default): keyless messages are batched per producer instance
  and spread by the sticky partitioner.
- `strategy=custom`: the app's `SegmentPartitioner` pins `premium-*` keys to
  partition 0 and rotates everything else round-robin:

```bash
curl -X POST 'localhost:8080/api/orders?strategy=custom' -H 'Content-Type: application/json' \
  -d '{"product":"vip-seat","amount":500,"key":"premium-42"}'
# -> partition 0
```

### 3. Multi-cluster failover (MirrorMaker 2 + health router)

```bash
# health view
curl localhost:8080/api/orders/clusters

# pin traffic explicitly
curl -X POST 'localhost:8080/api/orders?cluster=primary' ...
curl -X POST 'localhost:8080/api/orders?cluster=secondary' ...

# automatic failover: kill the primary broker and observe
docker stop kafka-primary
curl -X POST 'localhost:8080/api/orders?cluster=auto' ...
# -> response shows "cluster": "secondary"
docker start kafka-primary
```

MM2 mirrors `orders-topic` from A to B (same topic name via
`IdentityReplicationPolicy`) and syncs consumer group offsets, so a consumer
restarted against B (`app.consume-from=secondary`) resumes where A left off.
Check replication lag in Kafka UI while the primary is down.

## Configuration

`src/main/resources/application.yml`:

| Property | Meaning |
|---|---|
| `kafka.clusters.primary/secondary.bootstrap-servers` | cluster endpoints |
| `kafka.topic.name / partitions / replicas` | topic topology (default 6 partitions) |
| `app.instance-id` | identifier shown in rebalance/consume logs |
| `app.consume-from` | cluster consumers read from (`primary` \| `secondary`) |
| `app.listener-concurrency` | threads per listener container |

## Tests

```bash
./gradlew test        # unit (partitioner, failover router) + embedded-Kafka integration tests
```

## Project layout

```
infra/mm2.properties                       MirrorMaker 2 config
docker-compose.yml                         2 Kafka clusters + MM2 + Kafka UI
src/main/kotlin/com/example/kafkalb/
  config/                                  topic + cluster beans, properties
  producer/                                failover-aware producer, custom partitioner, JSON codec
  consumer/                                group listeners + rebalance logging
  failover/                                cluster health probes + routing decision
  api/                                     REST endpoints
  partitioner/                             SegmentPartitioner (premium pin + round-robin)
```
