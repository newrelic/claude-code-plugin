# Kafka investigation guide

What each health concept can mean, and the check that tells the causes apart. Read the entry for every concept that fired. A fired concept names a state; the checks below look for the mechanism. Report what the checks established and name the rest unconfirmed.

Checks that need the Kafka command-line tools, the MSK operation history or change records have no tool here: list them as operator-side steps.

Contents: Consumer lag · Retention · Replication and availability · Controller · A broker stopped reporting · Latency and throttling · Resources · Client connection and access failures · Skew and capacity · Confluent Cloud capacity · Nothing fired · Symptoms with no concept · A whole source went quiet.

Provider limits that apply to every entry:

- **nri-kafka and OpenTelemetry** report from the broker itself, so a stale metric can mean the broker stopped.
- **AWS MSK** reports through AWS: there is no host or container, AWS patches and replaces brokers (a short gap can be transient), and data arrives minutes late.
- **Confluent Cloud** has no brokers or hosts. Reachable causes are capacity, quotas, key skew, client settings and access keys.

## Consumer lag

Concepts: `consumer_group_lag`, `lag_concentration`, `consumer_progress`, `consumer_group_members`, `records_lag_max`.

- **Stalled or failing to commit:** `consumer_progress` frozen while sibling partitions move. Confirm in the application's logs.
- **Poison message:** one partition's lag rises with its progress frozen while the others drain (`FACET partition`).
- **Slow consumer or starved host:** `fetch_latency`, `app_host_cpu`. A throttled consumer stops heartbeating while it still reports.
- **Producer burst:** compare `throughput_in` with the consumer's rate. Steady producers with falling consumer throughput point at the consumer; a producer spike with steady consumers is an upstream burst.
- **Offset reset:** lag drops abruptly with no matching consumption, or the application logs an out-of-range error. Report each onset time.
- **Shrunk or empty group:** `consumer_group_members`.
- **Schema break:** deserialization errors (`SerializationException`, "Unknown magic byte") on every consumer instance and every partition, usually right after a producer change. One partition with the others healthy is a poison message instead. Report the exact onset and exception type for the producer's owner; if the evidence is gradual or on one partition, drop this reading.
- **Rebalance storm:** no concept fires. Look for flapping membership, rising `connection_close_rate`, and lag rising while no partition progresses. Compare rebalance times with deploys, the poll interval with `max.poll.interval.ms`, and missed heartbeats with `session.timeout.ms`; the consumer host's CPU explains the heartbeat misses.
- **Gaps:** MSK and Confluent Cloud report lag only for stable groups, so absent lag is not healthy. On nri-kafka a group that died stops appearing in `KafkaOffsetSample`.

## Retention

Concept: `retention_margin` (OpenTelemetry only). It compares lag with the retention period. Causes: a configuration or throughput change, a recreated topic. Confirm loss from the consumer: a reset, a skip, or an out-of-range error in its logs.

## Replication and availability

Concepts: `under_replicated`, `under_min_isr`, `offline_partitions`, `replica_fetcher_lag`, `leader_concentration`.

- **Meaning:** under-replicated partitions have followers behind the leader. Offline partitions have no leader. Under-min-ISR keeps the leader but rejects `acks=all` writes.
- **Dead leader:** it cannot report its own under-replication, so `under_replicated` can read 0 while partitions are offline. Read it with `offline_partitions` and the leader count per broker before calling replication healthy.
- **Stopped broker:** see "A broker stopped reporting".
- **Slow follower:** `host_cpu`, `host_disk`, `broker_jvm_gc`.
- **With every broker reporting:** a failed log directory on a live broker (`KafkaStorageException` in its log), every in-sync member restarting at once, or a topic with a single replica.
- **Pattern (a hint, not a finding):** spiky suggests undersized brokers; constant at low traffic suggests access rules blocking replication. Confirm with `host_cpu`, `host_disk` or the broker log.
- **Gaps:** MSK has no in-sync or replica-lag metric, and a patch or resize raises under-replication by design for a few minutes.

## Controller

Concept: `active_controller`. No broker reports itself controller.

- **Election in progress:** clears on its own.
- **Quorum loss:** KRaft needs a majority of `controller.quorum.voters`; ZooKeeper needs its ensemble. Name the unreachable voters or brokers, and flag a quorum that cannot lose a member.
- **Metric artifact:** not faceted per broker, or no controller-role node scraped.
- **Gaps:** MSK's metric can be cluster-level; a maximum of 0 or a falling sample count is the signal.

## A broker stopped reporting

Concepts: `connection_close_rate`, `consumer_connection_count`; the topology shows `reporting: false`.

- **Stopped cleanly:** exit code 143. **Killed:** exit 137; say out of memory only with an `OOMKilled` reason or memory evidence.
- **Restart loop:** repeated restarts in the container or pod history ("Hosts, containers and Kubernetes" in `queries.md`).
- **Exhaustion before the stop:** `broker_disk`, `broker_jvm_gc`.
- **Host or collector lost:** the host's other signals stopped too. A collector on an isolated host makes a healthy broker read stale, so check whether anything else on the host reports.
- **Zero consumer connections while telemetry is fresh:** check host or container CPU limits before blaming the network.
- **Change at that time:** a deploy or configuration change (operator-side).
- **Gaps:** a broker already down when the window opened, or all brokers stopping together, reads clean. Stale metrics prove a stop only where the broker reports itself (nri-kafka, OpenTelemetry); on MSK and Confluent Cloud they show the pipeline.

## Latency and throttling

Concepts: `request_latency`, `fetch_latency`, `request_failures`, `producer_throttle`.

- **Disk or CPU pressure:** `host_disk`, `host_cpu`, `broker_jvm_gc`.
- **Handler saturation:** request-handler idle percent and request-queue time (discover the metric name with `metricName LIKE '%idle%' OR metricName LIKE '%queue%'`).
- **Slow `acks=all` writes:** `under_replicated`. Under-min-ISR causes errors, not slowness.
- **Idle consumer:** fetch requests wait up to `fetch.max.wait.ms` (default 500 ms), so an idle consumer reads about that on a healthy cluster.
- **MSK:** latency is a mean split into local, remote, request-queue and response-queue time; the largest part names the stage. Other MSK causes: CPU-credit exhaustion (`aws.kafka.CPUCreditBalance` near 0), volume throughput limits, and network allowance (`TrafficShaping`).
- **Throttling:** quota values are not telemetry (`kafka-configs --describe` is operator-side). MSK and Confluent Cloud export no broker throttle time; on Confluent Cloud read the client's throttle-time measures.
- **Gaps:** `fetch_latency` is OpenTelemetry only; `request_failures` is OpenTelemetry and nri-kafka.

## Resources

Concepts: `broker_disk`, `broker_jvm_gc`, `host_cpu`, `host_memory`, `host_disk`, `app_host_cpu`, `app_host_memory`, `app_host_disk`.

- **Disk growth:** a fast topic, long retention or large segments, `__consumer_offsets`, a compaction cleaner behind, reassignment traffic, or non-Kafka files.
- **Heap:** a post-collection heap that rises across days is a leak; one that follows throughput is load. A window under a day cannot separate them, so say so.
- **Application starvation:** `app_host_*` firing means the application may be starved rather than the broker slow. A host shared with a broker fires both; confirm with `analyze_kafka_client_metrics` on the application.
- **Gaps:** OpenTelemetry has no broker-scoped disk metric, so read `host_disk`. MSK reports no host or container.

## Client connection and access failures

No concept covers these; the evidence is in the broker and application logs ("Log searches" in `queries.md`).

- **TLS handshake failures:** `SSLHandshakeException`, "certificate expired", or an unknown CA or truststore error. A bare connectivity loss does not show an expired certificate, so name a certificate cause only when the log text says so. Checking the certificate itself is operator-side.
- **Authentication or authorization failures:** `SaslAuthenticationException`, `TopicAuthorizationException`, "Failed authentication". Facet by host. An onset with no deploy points to a credential or access-rule change (operator-side).
- **`ProducerFencedException`:** a transactional producer started while its predecessor was still active, for example during a rolling restart. It is restart timing, not broker capacity.

## Skew and capacity

Concepts: `leader_concentration`, `hot_partition`, `cluster_load`.

- **Leadership on one broker:** read the leader count per broker in the broker health query. Report `leader_concentration` as a risk to watch, not a fault. If a broker was down, skew left behind is its aftermath: see "A broker stopped reporting".
- **Hot partition:** one partition carrying most of a topic's traffic. Rank per-partition or per-topic throughput. A lag alert on a single partition is a consumer-lag question, not a hot partition.
- **Headroom:** read the peak figure as a `TIMESERIES` over several days (throughput, disk growth, CPU, heap per broker), extrapolate, and state the assumption and the headroom as a percentage. Settings such as retention, quotas and partition counts are not telemetry.
- **Gaps:** `leader_concentration` can be unmeasured on older nri-kafka builds. Report an unavailable signal as unavailable, never as "balanced".

## Confluent Cloud capacity

Concepts: `cluster_load`, `hot_partition`, `connector_state`. They show `unavailable`, so read the raw metrics: cluster load percent (a fraction from 0 to 1), per-topic received bytes, connector status, and a drop in successful authentications for a principal. When many applications fail briefly at once, tell the user to check the provider's status page. Confluent Cloud data arrives minutes late, so a fresh gap is delivery lag, not an outage.

## Nothing fired

List what was unmeasured, name any latent risk (a single replica, few partitions), and say "nothing fired among the measured concepts", not "healthy".

## Symptoms with no concept

Handler saturation, unclean elections, the log cleaner, ZooKeeper session loss and the client failures above have no health concept. Search by keyword: `metricName LIKE '%idle%'`, `'%unclean%'`, `'%cleaner%'`, `'%zookeeper%'`, `'%raft%'`, or the log searches in `queries.md`. Metrics for Kafka Connect, MirrorMaker and Schema Registry come from their own integrations; if `uniques(metricName)` shows none for the account, say no data was found rather than a cause.

## A whole source went quiet

A loss-of-signal alert is not evidence of a Kafka fault, and not evidence of health either. Every metric from one source or host stops, so the cause is the collector or its delivery path, not Kafka. Find the gap with the last report per source and host, including the hosts' own metrics (`latest(timestamp)` over a lookback that reaches before the gap, such as a day), then attribute it by scope:

| Scope of the gap | Points at |
|---|---|
| Every signal from this host or pod, Kafka and non-Kafka | the collector or agent process, its egress, or the host |
| Every scrape-derived Kafka signal while the broker's JMX and the applications keep flowing | one receiver in the collector pipeline, not the cluster |
| One entity's signals while its peers keep reporting | that entity is gone, restarted or renamed |
| One label or facet vanishing while the series continues | a relabel or transform step, not the broker |
| Everything from this account or region at once | the ingest side or the network, not this cluster |

- A broker marked not reporting is not a broker-down verdict: a collector or egress fault does the same. Call the broker down only when the survivors show its departure (broker count drop, under-replication, leadership moving) or its container or host is confirmed down.
- Do not answer with the fault the alert condition was written for: a signal-loss violation on a "heap is high" condition says nothing about heap.
- The telemetry does not show planned maintenance. Say a maintenance cause was not ruled out instead of reporting an incident.
- If the account has `NrIntegrationError` events, count them over the window by message.
