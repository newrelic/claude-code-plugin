# Kafka queries

NRQL for `SKILL.md`. Replace `<cluster>`, `<id>`, `<group>` and `<app>` with real values. Every query is read-only.

Contents: Conventions · What exists (and "Is a broker reporting, and which partitions") · Broker health · Consumer lag · Throughput · Request latency · Host and JVM (and "Hosts, containers and Kubernetes") · Client applications · Trends and capacity · Log searches · A measure that is not listed · Identity attributes. Read Conventions and the sections the question needs.

## Conventions

- Scope every query by the provider's cluster attribute (identity table at the end). For OpenTelemetry also add `instrumentation.provider = 'opentelemetry'`.
- Backtick metric names, and attributes that hold a hyphen or a reserved word (`group`). Alias every aggregate, never to a reserved word (`AS end_offset`, not `AS offset`).
- Pick the aggregate by metric type: gauge `latest()`; counter `rate(sum(<m>), 1 second)`; AWS MSK stream and Confluent Cloud summaries `average()`, `max()` or `sum()`, never `latest()`.
- Put metrics in one `SELECT` only when they share dimensions. A metric with an extra dimension (a request `type`, a `topic`) gets its own query.
- `Metric` rows carry `entity.guid`; Sample events carry `entityGuid`.
- `KafkaBrokerSample` mixes broker rows and per-topic rows: `topic IS NULL` for broker figures, `topic IS NOT NULL` for per-topic ones.
- No `UTC` suffix on `SINCE` or `UNTIL`. Count with `uniqueCount(attr)`, not `count(DISTINCT attr)`.

## What exists

Provider probes (the one with a count above 0 is the provider):

```nrql
FROM Metric SELECT count(*) WHERE `kafka.cluster.name` = '<cluster>' AND instrumentation.provider = 'opentelemetry' SINCE 1 hour ago
```
```nrql
FROM KafkaBrokerSample SELECT count(*) WHERE clusterName = '<cluster>' SINCE 1 hour ago
```
```nrql
FROM Metric SELECT count(*) WHERE `aws.kafka.ClusterName` = '<cluster>' SINCE 1 hour ago
```
```nrql
FROM Metric SELECT count(*) WHERE `confluent.clusterId` = '<id>' SINCE 1 hour ago
```

With only a name or no name, list what the account has: `SHOW EVENT TYPES`, then `SELECT uniques(<cluster attribute>) FROM <source> SINCE 1 hour ago` for each provider. If several match, ask which one was meant.

With no cluster named ("how many brokers do I have"), facet by the cluster attribute, one query per provider that has data, and say which clusters the answer covers:

```nrql
-- OpenTelemetry
FROM Metric SELECT uniqueCount(`broker.id`) AS brokers, uniqueCount(topic) AS topics FACET `kafka.cluster.name`
WHERE `kafka.cluster.name` IS NOT NULL AND instrumentation.provider = 'opentelemetry' SINCE 1 hour ago LIMIT 100
```
```nrql
-- nri-kafka
FROM KafkaBrokerSample SELECT uniqueCount(entityName) AS brokers FACET clusterName WHERE topic IS NULL SINCE 1 hour ago LIMIT 100
```
```nrql
-- AWS MSK
FROM Metric SELECT uniqueCount(`aws.kafka.BrokerID`) AS brokers FACET `aws.kafka.ClusterName`
WHERE `aws.kafka.ClusterName` IS NOT NULL SINCE 1 hour ago LIMIT 100
```
Confluent Cloud exposes no brokers; count its topics per `confluent.clusterId` the same way.

Counts: use `uniqueCount` of the identifying attribute. Topic names starting `__` are internal; say whether a count includes them.

```nrql
-- OpenTelemetry
FROM Metric SELECT uniqueCount(`broker.id`) AS brokers, uniqueCount(topic) AS topics, uniqueCount(`group`) AS groups
WHERE kafka.cluster.name = '<cluster>' AND instrumentation.provider = 'opentelemetry' SINCE 1 hour ago
```
```nrql
-- nri-kafka (brokers and topics are different event types)
FROM KafkaBrokerSample SELECT uniqueCount(entityName) AS brokers WHERE clusterName = '<cluster>' AND topic IS NULL SINCE 1 hour ago
FROM KafkaTopicSample SELECT uniqueCount(entityName) AS topics WHERE clusterName = '<cluster>' SINCE 1 hour ago
-- if that returns no rows, count topics from the per-topic broker rows
FROM KafkaBrokerSample SELECT uniqueCount(topic) AS topics WHERE clusterName = '<cluster>' AND topic IS NOT NULL SINCE 1 hour ago
```
```nrql
-- AWS MSK
FROM Metric SELECT uniqueCount(`aws.kafka.BrokerID`) AS brokers, uniqueCount(`aws.kafka.Topic`) AS topics
WHERE `aws.kafka.ClusterName` = '<cluster>' SINCE 1 hour ago
```
```nrql
-- Confluent Cloud (no brokers to count)
FROM Metric SELECT uniqueCount(`confluent.kafka.server.metric.topic`) AS topics, average(`confluent.kafka.server.partition_count`) AS partitions
WHERE `confluent.clusterId` = '<id>' SINCE 1 hour ago
```

### Is a broker reporting, and which partitions

No metric says a broker is down: read when each broker last reported. An old time means it stopped. Use a lookback that reaches before the gap (`SINCE 1 day ago`): the default one-hour window drops a broker that has been silent longer than that. Metrics are coarser than "which partitions", so answer at the grain the provider has and say what it cannot show.

```nrql
-- OpenTelemetry (kafka.partition.* carry only broker.id, never topic or partition)
FROM Metric SELECT latest(timestamp) AS last_seen FACET `broker.id`
WHERE kafka.cluster.name = '<cluster>' AND instrumentation.provider = 'opentelemetry' AND metricName = 'kafka.partition.count' SINCE 1 day ago
```
```nrql
-- nri-kafka (no partition rows; the finest grain is the topic. topic IS NULL keeps broker rows)
FROM KafkaBrokerSample SELECT latest(timestamp) AS last_seen FACET entityName
WHERE clusterName = '<cluster>' AND topic IS NULL SINCE 1 day ago

FROM KafkaTopicSample SELECT max(topic.underReplicatedPartitions) AS under_replicated FACET entityName
WHERE clusterName = '<cluster>' SINCE 15 minutes ago
```
```nrql
-- AWS MSK (figures are per broker; none names a topic or partition)
FROM AwsMskBrokerSample SELECT latest(timestamp) AS last_seen FACET provider.brokerId
WHERE provider.clusterName = '<cluster>' SINCE 1 day ago
```
MSK data arrives minutes late and AWS replaces brokers, so a short gap is not a stop.

To tell a stopped broker from a stopped collector, read the broker hosts' own last report the same way:

```nrql
FROM SystemSample SELECT latest(timestamp) AS last_seen FACET hostname WHERE hostname IN ('<host>') SINCE 1 day ago
```

Confluent Cloud does not expose brokers, so none can be reported down; its metrics carry `topic`, never `partition`.

## Broker health

```nrql
-- OpenTelemetry (active controller is 1 on one broker and 0 on the others: read it per broker)
FROM Metric SELECT latest(`kafka.partition.count`) AS partitions, latest(`kafka.partition.under_replicated`) AS under_replicated,
  latest(`kafka.partition.under_min_isr`) AS under_min_isr, latest(`kafka.controller.active.count`) AS active_controller,
  latest(`kafka.max.lag`) AS replica_max_lag, latest(`kafka.request.queue`) AS request_queue,
  latest(`jvm.memory.heap.used`) / latest(`jvm.memory.heap.max`) * 100 AS heap_pct,
  rate(sum(`jvm.gc.collections.elapsed`), 1 minute) AS gc_per_min, rate(sum(`kafka.message.count`), 1 second) AS msgs_per_sec,
  latest(`kafka.broker.leader.count`) AS leaders, rate(sum(`kafka.unclean.election.rate`), 1 minute) AS unclean_per_min
FACET `broker.id` WHERE kafka.cluster.name = '<cluster>' AND instrumentation.provider = 'opentelemetry' SINCE 15 minutes ago
```
```nrql
-- OpenTelemetry: offline partitions have no broker dimension, so query them unfaceted. Failed requests carry a request type
FROM Metric SELECT latest(`kafka.partition.offline`) AS offline_partitions
WHERE kafka.cluster.name = '<cluster>' AND instrumentation.provider = 'opentelemetry' SINCE 15 minutes ago
FROM Metric SELECT rate(sum(`kafka.request.failed`), 1 minute) AS failed_per_min FACET `type`
WHERE kafka.cluster.name = '<cluster>' AND instrumentation.provider = 'opentelemetry' SINCE 15 minutes ago
```
```nrql
-- nri-kafka
FROM KafkaBrokerSample SELECT latest(replication.unreplicatedPartitions) AS under_replicated,
  average(replication.isrShrinksPerSecond) AS isr_shrinks_ps, average(broker.messagesInPerSecond) AS msgs_in_ps,
  average(broker.IOInPerSecond) AS bytes_in_ps, average(broker.IOOutPerSecond) AS bytes_out_ps,
  average(request.handlerIdle) AS handler_idle, average(request.produceRequestsFailedPerSecond) AS produce_failed_ps
FACET entityName WHERE clusterName = '<cluster>' AND topic IS NULL SINCE 15 minutes ago
```
```nrql
-- AWS MSK, metric stream. ActiveControllerCount and OfflinePartitionsCount carry no broker: query them without the facet
FROM Metric SELECT max(`aws.kafka.UnderReplicatedPartitions`) AS under_replicated, max(`aws.kafka.UnderMinIsrPartitionCount`) AS under_min_isr,
  average(`aws.kafka.PartitionCount`) AS partitions, average(`aws.kafka.LeaderCount`) AS leaders,
  average(`aws.kafka.CpuUser`) AS cpu_user, average(`aws.kafka.MemoryUsed`) AS mem_used,
  max(`aws.kafka.KafkaDataLogsDiskUsed`) AS data_disk_pct
FACET `aws.kafka.BrokerID` WHERE `aws.kafka.ClusterName` = '<cluster>' SINCE 15 minutes ago
```
```nrql
-- AWS MSK, polled (latency and throughput facet by provider.brokerId, the rest by entityName)
FROM AwsMskBrokerSample SELECT average(provider.underReplicatedPartitions.Sum) AS under_replicated,
  average(provider.partitionCount.Average) AS partitions, average(provider.leaderCount.Average) AS leaders,
  average(provider.cpuUser.Average) AS cpu_user, average(provider.memoryUsed.Average) AS mem_used,
  average(provider.kafkaDataLogsDiskUsed.Average) AS data_disk_pct
FACET entityName WHERE provider.clusterName = '<cluster>' SINCE 15 minutes ago
```
```nrql
-- Confluent Cloud
FROM Metric SELECT average(`confluent.kafka.server.active_connection_count`) AS connections,
  sum(`confluent.kafka.server.request_count`) AS requests, sum(`confluent.kafka.server.request_bytes`) AS request_bytes,
  sum(`confluent.kafka.server.response_bytes`) AS response_bytes
FACET `confluent.clusterId` WHERE `confluent.clusterId` = '<id>' SINCE 15 minutes ago
```

## Consumer lag

```nrql
-- OpenTelemetry
FROM Metric SELECT latest(`kafka.consumer_group.lag_sum`) AS lag FACET `group`, topic
WHERE kafka.cluster.name = '<cluster>' AND instrumentation.provider = 'opentelemetry' SINCE 15 minutes ago
```
```nrql
-- nri-kafka
-- per partition: add the partitions up for a topic total
FROM KafkaOffsetSample SELECT latest(`consumer.lag`) AS lag
FACET consumerGroup, topic, partition WHERE clusterName = '<cluster>' SINCE 15 minutes ago
```
```nrql
-- nri-kafka group total (it has no topic)
FROM KafkaOffsetSample SELECT latest(`consumerGroup.totalLag`) AS group_total_lag
FACET consumerGroup WHERE clusterName = '<cluster>' SINCE 15 minutes ago
```
```nrql
-- AWS MSK
FROM Metric SELECT max(`aws.kafka.MaxOffsetLag`) AS max_offset_lag, max(`aws.kafka.EstimatedMaxTimeLag`) AS max_time_lag
FACET `aws.kafka.ConsumerGroup`, `aws.kafka.Topic` WHERE `aws.kafka.ClusterName` = '<cluster>' SINCE 15 minutes ago
```
```nrql
-- Confluent Cloud
FROM Metric SELECT max(`confluent.kafka.server.consumer_lag_offsets`) AS max_lag
FACET `confluent.kafka.server.metric.consumer_group_id`, `confluent.kafka.server.metric.topic`
WHERE `confluent.clusterId` = '<id>' SINCE 15 minutes ago
```

Rebalances come from the consumer app's client metrics:

```nrql
-- OpenTelemetry
FROM Metric SELECT rate(sum(`kafka.consumer.rebalance_total`), 1 minute) AS rebalances_per_min,
  rate(sum(`kafka.consumer.failed_rebalance_total`), 1 minute) AS failed_per_min
WHERE `service.name` = '<app>' SINCE 1 hour ago
```
```nrql
-- APM agent
FROM Metric SELECT max(`apm.service.kafka.consumer-coordinator`) AS v WHERE appName = '<app>'
  AND measurement IN ('rebalance-total', 'last-rebalance-seconds-ago', 'assigned-partitions') FACET measurement SINCE 1 hour ago
```

## Throughput

```nrql
-- OpenTelemetry, per broker
FROM Metric SELECT rate(sum(`kafka.message.count`), 1 second) AS msgs_per_sec, rate(sum(`kafka.network.io`), 1 second) AS net_bytes_per_sec
FACET `broker.id` WHERE kafka.cluster.name = '<cluster>' AND instrumentation.provider = 'opentelemetry' SINCE 15 minutes ago
```
```nrql
-- OpenTelemetry, per topic
FROM Metric SELECT rate(sum(`kafka.topic.io`), 1 second) AS bytes_per_sec FACET topic, direction
WHERE kafka.cluster.name = '<cluster>' AND instrumentation.provider = 'opentelemetry' SINCE 15 minutes ago
```
```nrql
-- nri-kafka, per topic (per broker is in Broker health)
FROM KafkaBrokerSample SELECT average(`broker.bytesWrittenToTopicPerSecond`) AS bytes_in_ps FACET topic
WHERE clusterName = '<cluster>' AND topic IS NOT NULL SINCE 15 minutes ago
```
```nrql
-- AWS MSK, per topic
FROM Metric SELECT average(`aws.kafka.BytesInPerSec.byTopic`) AS bytes_in_ps, average(`aws.kafka.BytesOutPerSec.byTopic`) AS bytes_out_ps
FACET `aws.kafka.Topic` WHERE `aws.kafka.ClusterName` = '<cluster>' SINCE 15 minutes ago
```
```nrql
-- Confluent Cloud, per topic
FROM Metric SELECT sum(`confluent.kafka.server.received_bytes`) AS bytes_in, sum(`confluent.kafka.server.sent_bytes`) AS bytes_out,
  max(`confluent.kafka.server.retained_bytes`) AS retained_bytes
FACET `confluent.kafka.server.metric.topic` WHERE `confluent.clusterId` = '<id>' SINCE 15 minutes ago
```

## Request latency

```nrql
-- OpenTelemetry, by request type
FROM Metric SELECT latest(`kafka.request.time.99p`) AS p99_ms, latest(`kafka.request.time.avg`) AS avg_ms FACET `type`
WHERE kafka.cluster.name = '<cluster>' AND instrumentation.provider = 'opentelemetry' AND `type` IN ('Produce', 'FetchConsumer', 'FetchFollower')
SINCE 15 minutes ago
```

- **nri-kafka:** `average(request.produceTime99Percentile)` and `average(request.fetchTime99Percentile)` from `KafkaBrokerSample` (`topic IS NULL`), by `entityName`.
- **AWS MSK:** ``average(`aws.kafka.ProduceTotalTimeMsMean`)`` and ``average(`aws.kafka.FetchConsumerTotalTimeMsMean`)`` by `aws.kafka.BrokerID`.
- **Confluent Cloud:** broker request latency is not reported. Check `keyset()` for a producer-side figure and say which one you read.
- End-to-end latency across producer, broker and consumer is `Span` data: query `percentile(duration.ms, 50, 95, 99)` on the producer and consumer services.

## Host and JVM

```nrql
-- OpenTelemetry, per host
FROM Metric SELECT latest(`jvm.system.cpu.utilization`) * 100 AS cpu_pct,
  latest(`jvm.memory.heap.used`) / latest(`jvm.memory.heap.max`) * 100 AS heap_pct
FACET `host.name` WHERE kafka.cluster.name = '<cluster>' AND instrumentation.provider = 'opentelemetry' SINCE 15 minutes ago
```
```nrql
-- OpenTelemetry, disk (the filesystem metric has no broker.id: facet by host; exclude read-only snap mounts)
FROM Metric SELECT filter(sum(`system.filesystem.usage`), WHERE `state` = 'used') / sum(`system.filesystem.usage`) * 100 AS disk_used_pct
FACET `host.name`, `mountpoint` WHERE kafka.cluster.name = '<cluster>' AND metricName = 'system.filesystem.usage'
AND `mountpoint` NOT LIKE '/snap/%' SINCE 15 minutes ago
```
```nrql
-- OpenTelemetry, container CPU limit (throttled / total near 1 means CPU was denied); present when the collector's docker_stats quota metrics are enabled
FROM Metric SELECT latest(`container.cpu.throttling_data.throttled_periods`) AS throttled, latest(`container.cpu.throttling_data.periods`) AS periods
FACET `container.name`, `host.name` WHERE `host.name` = '<host>' AND instrumentation.provider = 'opentelemetry' SINCE 15 minutes ago
```

- **nri-kafka:** take the broker hostnames from `SELECT uniques(hostname) FROM KafkaBrokerSample WHERE clusterName = '<cluster>'`, then `FROM SystemSample SELECT average(cpuPercent), average(memoryUsedPercent), max(diskUsedPercent) FACET hostname WHERE hostname IN (...)`.
- **AWS MSK:** CPU, memory and disk are in its Broker health query. **Confluent Cloud:** no host resources.

### Hosts, containers and Kubernetes (infrastructure agent events)

For nri-kafka brokers, any broker on a host with the infrastructure agent, and brokers in containers. Scope Docker brokers by `name`, `fullHostname` or `entityGuid`, Kubernetes brokers by `podName` or `entityGuid`, and hosts by `hostname`. Add `SINCE 15 minutes ago` unless the query sets a window.

```nrql
-- CPU, iowait, memory and load per host (iowait is part of cpuPercent; compare load with coreCount)
FROM SystemSample SELECT max(cpuPercent) AS cpu, max(cpuIOWaitPercent) AS iowait, max(memoryUsedPercent) AS mem,
  latest(loadAverageFiveMinute) AS load5, latest(coreCount) AS cores FACET hostname WHERE hostname IN ('<host>') SINCE 15 minutes ago
```
```nrql
-- Disk per mount: take the maximum, never an average across mounts, which hides one full log volume. Busy time is a separate figure.
FROM StorageSample SELECT max(diskUsedPercent) AS used, min(diskFreePercent) AS free, max(totalUtilizationPercent) AS io_busy
FACET hostname, mountPoint WHERE hostname IN ('<host>') SINCE 15 minutes ago
```
```nrql
-- Docker: CPU and memory against the container's own limit, and restarts
FROM ContainerSample SELECT latest(cpuUsedCoresPercent) AS cpu_of_limit, latest(cpuLimitCores) AS limit_cores,
  latest(memoryUsageBytes) AS mem_bytes, latest(memorySizeLimitBytes) AS mem_limit, latest(restartCount) AS restarts
FACET name WHERE name LIKE '<broker container>%' SINCE 15 minutes ago
```
```nrql
-- Kubernetes: use, percent of the container limit, and CPU throttling (sum over sum, never an average of ratios)
FROM K8sContainerSample SELECT max(cpuUsedCores) AS cores, max(memoryWorkingSetBytes) AS ws_bytes,
  max(cpuCoresUtilization) AS cpu_pct, max(memoryWorkingSetUtilization) AS mem_pct,
  sum(containerCpuCfsThrottledPeriodsDelta) / sum(containerCpuCfsPeriodsDelta) * 100 AS throttled_pct
FACET podName, containerName WHERE podName LIKE '<broker pod>%' SINCE 15 minutes ago
```
```nrql
-- Kubernetes: why a container restarted (the last termination) and how many restarts fell in the window
FROM K8sContainerSample SELECT sum(restartCountDelta) AS restarts, sum(containerOOMEventsDelta) AS oom_kills,
  latest(lastTerminatedExitReason) AS last_reason, latest(lastTerminatedExitCode) AS last_exit_code
FACET podName, containerName WHERE podName LIKE '<broker pod>%' SINCE 1 hour ago
```

## Client applications

Use the source the app reports. A producer:

```nrql
-- OpenTelemetry producer: the counters sum across the app's clients
FROM Metric SELECT rate(sum(`kafka.producer.record_send_total`), 1 second) AS send_rate,
  rate(sum(`kafka.producer.record_error_total`), 1 second) AS error_rate,
  rate(sum(`kafka.producer.record_retry_total`), 1 second) AS retry_rate, average(`kafka.producer.request_latency_avg`) AS latency_ms
WHERE `service.name` = '<app>' SINCE 15 minutes ago
```

A consumer:

```nrql
-- OpenTelemetry consumer
FROM Metric SELECT max(`kafka.consumer.records_lag_max`) AS lag_max, average(`kafka.consumer.fetch_latency_avg`) AS fetch_ms,
  max(`kafka.consumer.last_poll_seconds_ago`) AS secs_since_poll
WHERE `service.name` = '<app>' SINCE 15 minutes ago
```
```nrql
-- APM agent: `measurement` picks the figure; APM queries need appName or entity.guid
FROM Metric SELECT max(`apm.service.kafka.consumer`) AS v WHERE appName = '<app>'
  AND measurement IN ('last-poll-seconds-ago', 'time-between-poll-max', 'poll-idle-ratio-avg') FACET measurement SINCE 15 minutes ago
```
```nrql
-- APM agent consumer lag, every topic
FROM Metric SELECT max(`apm.service.kafka.consumer-fetch-manager`) AS lag WHERE appName = '<app>'
  AND measurement LIKE '%/records-lag' FACET measurement SINCE 15 minutes ago
```

List the measurement names an APM app reports: `SELECT uniques(measurement) FROM Metric WHERE metricName = '<metric>' AND appName = '<app>'` (metrics: `apm.service.kafka.producer`, `.consumer`, `.consumer-fetch-manager`, `.consumer-coordinator`). An app with no client metrics returns no rows; that does not show the app is healthy. AWS MSK and Confluent Cloud report broker and cluster data only.

## Trends and capacity

Compare with an earlier period, or read a multi-day trend of a peak figure and extrapolate it, stating the assumption and the headroom as a percentage.

```nrql
-- OpenTelemetry throughput per broker against an hour earlier
FROM Metric SELECT rate(sum(`kafka.message.count`), 1 second) AS msgs_per_sec FACET `broker.id`
WHERE kafka.cluster.name = '<cluster>' AND instrumentation.provider = 'opentelemetry' SINCE 1 hour ago COMPARE WITH 1 hour ago
```
```nrql
-- OpenTelemetry throughput per broker over a week, hourly
FROM Metric SELECT rate(sum(`kafka.message.count`), 1 second) AS msgs_per_sec FACET `broker.id`
WHERE kafka.cluster.name = '<cluster>' AND instrumentation.provider = 'opentelemetry' SINCE 7 days ago TIMESERIES 1 hour
```
Use the same shape for the other providers' throughput, disk, CPU and heap queries above.

## Log searches

For a symptom with no health concept (authentication or authorization failures, TLS handshake errors, deserialization errors). Scope by the entity's GUID in `entity.guids`, which native infrastructure-agent, OpenTelemetry and APM log rows all carry (`entity.guid` is on OpenTelemetry and APM rows only). Start with the presence query; if it finds nothing with `=`, retry once with `LIKE '%<guid>%'`. Zero rows after the retry means no logs reach this GUID, not that its logs are clean. Truncate the message, and quote only the error class and a short excerpt.

```nrql
FROM Log SELECT count(*) WHERE entity.guids = '<guid>' SINCE 1 hour ago
```
```nrql
FROM Log SELECT count(*) FACET entity.guids
WHERE entity.guids IN ('<broker or application guid>')
  AND (message LIKE '%SaslAuthenticationException%' OR message LIKE '%TopicAuthorizationException%' OR message LIKE '%SSLHandshakeException%' OR message LIKE '%SerializationException%')
SINCE 1 hour ago LIMIT 20
```
```nrql
FROM Log SELECT substring(message, 0, 300) AS excerpt, timestamp
WHERE entity.guids = '<guid>' AND message LIKE '%<error class>%' SINCE 1 hour ago LIMIT 10
```

## A measure that is not listed

1. **Find the metric.** `SELECT uniques(metricName, 1000) FROM Metric WHERE <cluster attribute> = '<id>' SINCE 1 day ago` lists what the cluster reports, including custom names. Or `metricName LIKE '%<word>%'`; `SHOW EVENT TYPES` for events.
2. **Check its dimensions.** `SELECT keyset() FROM Metric WHERE metricName = '<name>'` returns its type and attributes; `SELECT keyset() FROM <EventType>` does the same for an event type. Always filter by metric or prefix, never `keyset()` across a whole cluster.
3. **Write it** in the shape of the nearest query above, aggregated by type, scoped by the cluster attribute. Run it with a short window and a `LIMIT` first.
4. **If it returns nothing,** check in order: the scope attribute, the provider filter, the window, then whether the metric exists for this scope.
5. **Report** that it is an ad hoc read of a metric these queries do not cover, and name it. The `unit` attribute is often empty; say the unit is unknown unless the name makes it clear.

## Identity attributes

| Concept | OpenTelemetry | nri-kafka | AWS MSK | Confluent Cloud |
|---|---|---|---|---|
| cluster | `kafka.cluster.name` | `clusterName` | `aws.kafka.ClusterName` / `provider.clusterName` | `confluent.clusterId` |
| broker | `broker.id` | `entityName` (`brokerId` on newer builds) | `aws.kafka.BrokerID` / `provider.brokerId` | none |
| topic | `topic` | `topic`, `entityName` on `KafkaTopicSample` | `aws.kafka.Topic` / `provider.topic` | `confluent.kafka.server.metric.topic` |
| consumer group | `group` | `consumerGroup` | `aws.kafka.ConsumerGroup` | `confluent.kafka.server.metric.consumer_group_id` |
| host | `host.name` | `hostname` | none | none |
| application | `service.name`, `client-id` | `entityName` (`consumer:<app>`) | none | `confluent.kafka.server.metric.client_id` |
