# Lesson 1: The log

**Goal:** understand what Kafka stores, how it spreads data across partitions, and how consumers share work and remember where they are.

## The idea

Kafka stores an **append-only log** of events. Producers add to the end. Consumers read from any position and track their own place. Reading never deletes anything; data leaves only when retention expires (7 days by default).

If you know databases: it is like the Postgres write-ahead log (WAL), but many independent readers can follow it.

| Term | Meaning |
|---|---|
| **Topic** | A named log, like `orders`. Similar to a table name. |
| **Partition** | A topic is split into N separate logs. The unit of parallelism and of ordering. |
| **Offset** | A record's position inside one partition: 0, 1, 2... Each partition counts on its own. |
| **Broker** | A Kafka server. Partitions are spread across brokers. |
| **Consumer group** | Consumers that share one job. Each partition goes to exactly one member of the group. |

```
topic: orders (3 partitions)

P0: [0][1][2][3][4]      <- producer appends here
P1: [0][1][2]
P2: [0][1][2][3]

group "order-tracker":  C1 reads P0+P1, C2 reads P2    committed: P0=3, P1=2, P2=4
group "invoicing":      C3 reads all                    committed: P0=1, P1=0, P2=1
```

Two groups read the same data without affecting each other. Each group's position is stored in Kafka itself, in an internal topic called `__consumer_offsets`.

### Rules that follow

1. **Order is only guaranteed inside one partition.** There is no order across partitions.
2. **The key picks the partition:** `hash(key) % number_of_partitions`. Same key, same partition, so events for one key stay in order.
3. **A group can use at most as many consumers as there are partitions.** Extra consumers sit idle.
4. **A committed offset is the next offset to read**, not the last one processed.

### Kafka vs a queue (RabbitMQ, SQS)

A queue deletes a message once it is consumed. Kafka keeps it, so you can add a new consumer later and replay history, rewind a broken consumer, and let many systems read one stream.

## Experiments

### 1. Send and receive

Window 1:

    uv run apps/order_consumer.py

Window 2:

    uv run apps/order_producer.py 10

Real output (window 2, shortened):

```
sent 1 x pad thai for arpit -> partition 1, offset 6
sent 1 x mushroom pizza for nicole -> partition 0, offset 2
sent 1 x mushroom pizza for sam -> partition 1, offset 8
sent 1 x chicken bowl for lara -> partition 0, offset 3
```

**What it shows:**
- `arpit` and `sam` always land in partition 1; `lara` and `nicole` always in partition 0. That is rule 2.
- Partition 2 got nothing. With only 4 distinct keys, `hash(key) % 3` happened to put 2 keys in each of 2 partitions. This is a **hot partition**: in production, adding consumers does not fix it. More distinct keys or a different key does.
- Offsets count per partition (P1 went 6 to 11 while P0 went 2 to 5). There is no topic-wide offset.

### 2. Two consumers in one group

Keep window 1 running. Window 3:

    uv run apps/order_consumer.py

Wait about 5 seconds, then run the producer again in window 2. Window 3 prints nothing. Check why:

    docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-tracker

Real output (columns trimmed):

```
TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
orders  0          11              11              0    kafka-python-...-39a079a3...
orders  1          17              17              0    kafka-python-...-39a079a3...
orders  2          0               0               0    kafka-python-...-5b10078e...
```

**What it shows:** kafka-python uses the **range assignor** by default. 3 partitions over 2 consumers is 1 each, plus the leftover to the first, so the first consumer got P0 and P1 and the second got only P2, which is empty. Rule 3 in action. `LAG 0` means each consumer has read everything in its partitions.

### 3. Clean failover

Press `Ctrl-C` in window 1. Wait about 5 seconds, then run the producer again.

Real output (window 3):

```
partition 1, offset 17: sam ordered 3 x pad thai
partition 0, offset 11: lara ordered 3 x mushroom pizza
```

**What it shows:** window 3 took over all partitions and started at exactly the offsets window 1 had committed (P0=11, P1=17). Nothing lost, nothing repeated. `consumer.close()` committed the final offsets and left the group, so Kafka rebalanced at once. The group's position lives in Kafka, not in the process.

### 4. Crash: kill without cleanup

Run the producer in window 2. While it is sending, in window 4:

    pkill -9 -f apps/order_consumer.py

`kill -9` skips `close()`. Check the group:

```
TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders  0          15              18              3
orders  1          25              30              5
```

Window 3 had printed up to P0 offset 15, but the committed offset is 15, meaning "next to read is 15". The consumer auto-commits every 5 seconds, and it died after processing offset 15 but before the next commit.

Restart the consumer in window 3. Real output:

```
partition 0, offset 15: lara ordered 1 x mushroom pizza    <- printed twice: before and after the crash
partition 0, offset 16: lara ordered 1 x frozen yogurt
partition 0, offset 17: lara ordered 1 x pad thai
partition 1, offset 25: arpit ordered 1 x frozen yogurt
```

**What it shows:** after a crash, a consumer resumes from its last **commit**, not from what it last processed. Anything processed after the last commit is replayed. This is **at-least-once delivery**, Kafka's default. With auto-commit, up to 5 seconds of messages can repeat.

Ways to handle duplicates:
1. **Make processing idempotent** - dedupe on `order_id`. The usual answer.
2. **Commit manually after processing** - `enable_auto_commit=False` and `consumer.commit()`. Smaller replay window, still not zero.
3. **Transactions (exactly-once)** - lesson 5.

Also note: after `kill -9` the group showed "no active members" only after a delay. Without `close()`, Kafka notices a dead consumer only when `session.timeout.ms` (45 seconds by default) runs out. Until then, a new consumer joining the group can wait silently.

## Check questions

1. A topic has 4 partitions. A group has 6 consumers. How many sit idle?
2. You send `bid` events keyed by `auction_id`. Is the order of two bids on the same auction guaranteed? On different auctions?
3. A consumer crashes. Its committed offset on P0 is 50, but it had processed up to 57. Where does it resume, and what happens to 50-57?

<details>
<summary>Answers</summary>

1. **2.** Each partition goes to one consumer in a group, so 4 work and 2 wait. They are useful as hot standbys.
2. **Same auction: yes**, same key means same partition. **Different auctions: no**, they may be in different partitions.
3. **It resumes at 50** (the committed offset is the next one to read). Offsets 50-57 are processed a second time, so processing must tolerate duplicates.

</details>

## Common mistakes from tutorials

- "Kafka pushes events to consumers." No: consumers **pull** with `poll()`.
- "Make one partition per region and add consumers to the busy one." Partitions are not named; the key hash decides. And one partition can only feed one consumer per group.
- "`flush()` saves buffered events if the program crashes." It only helps on a normal exit. A hard crash loses the producer's buffer.
- "Topics are created automatically." Only if the broker allows it. Managed Kafka and most production clusters turn this off; create topics on purpose, with a chosen partition count.
