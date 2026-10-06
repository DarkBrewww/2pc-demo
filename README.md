# 2pc-demo

A sharded bank ledger: 9,000 accounts split across three clusters of three nodes. Each cluster keeps its shard consistent with Multi-Paxos, and a transfer between accounts in different shards commits atomically through two-phase commit between the cluster leaders. Nodes can be failed and brought back in the middle of a run, which exercises leader election, catch-up and aborts.

Written for a graduate distributed systems course (Fall 2025), with later fixes to leader election, log recovery and cross-shard routing.

## Where the data lives

| Cluster | Nodes | Accounts |
|---|---|---|
| C1 | n1, n2, n3 | 1–3000 |
| C2 | n4, n5, n6 | 3001–6000 |
| C3 | n7, n8, n9 | 6001–9000 |

Every account starts with a balance of 10. Each node stores its shard in its own SQLite file (`node_1.db` to `node_9.db`). All nine nodes run inside one process, each behind its own gRPC server on localhost:50051–50059. A cluster needs two of its three nodes up to make progress.

## Inside a cluster

One node leads each cluster. For a transfer within its shard the leader checks that neither account is locked and that the sender can cover the amount, locks both accounts and proposes the transfer for the next log slot. Once a majority has accepted it, the leader commits the slot and every replica executes the log in slot order.

The leader sends a heartbeat every 100 ms carrying its last executed slot, and a follower that is behind (one that just recovered, say) gets the missing committed entries pushed to it. A follower that hears nothing for 600–1000 ms, randomized, starts an election. Ballots are `round * 10 + node ID`, so two candidates can never hold the same ballot. Before a new leader serves anything it collects promises from a majority, adopts entries that some replica already committed, re-proposes accepted but uncommitted entries at the same slot and fills holes with no-ops.

Balance queries go to the leader, which answers from its own copy.

## Across clusters

The leader of the sender's cluster coordinates a cross-shard transfer.

1. The coordinator checks the sender's lock and balance, locks the sender and replicates a prepare record in its own cluster. Executing that record debits the sender and saves the old balance in a write-ahead log on every replica.
2. It sends PREPARE to the receiver's cluster, trying each node there until the current leader answers. The participant locks the receiver, replicates its own prepare record (credit plus WAL entry) and replies.
3. If the participant can't prepare because the receiver is locked, its cluster has no majority or it can't be reached, the coordinator replicates an abort record, which puts the old balances back from the WAL on every replica, and tells the participant to abort too.
4. Otherwise both clusters replicate a commit record, the WAL entries are dropped and the locks are released.

The client retries only when no node in a cluster can answer, for example during an election. A definite answer such as "insufficient balance" or "aborted" comes back as is, and a retried request gets the outcome already recorded in the replicated log instead of running twice.

## Running it

You need Go 1.25 or newer and a C compiler, because the SQLite driver uses cgo (MinGW-w64 works on Windows).

```
go build -o distributed-txn.exe .
./distributed-txn.exe tests/test_input.csv
```

Press Enter to run each set. When it finishes, a menu accepts:

```
PrintBalance(<account>)   the account's balance on each node of its cluster
PrintDB                   balances of the accounts the set touched, on every node of their clusters
PrintView                 leader changes (NEW-VIEW messages) each node has seen
Performance               client throughput and average latency for the set
RunBenchmark              SmallBank or YCSB-style workloads
PrintReshard              suggested account moves based on the transfers so far
7 or Enter                go on to the next set
```

Balances are reset to 10 at startup and between sets.

## Test file format

```
Set Number,Transactions,Live Nodes
2,"(3001,4650,2)","[n_1,n_2,n_3,n_4,n_5,n_6,n_7,n_8,n_9]"
2,"F(n_3)","[n_1,n_2,n_3,n_4,n_5,n_6,n_7,n_8,n_9]"
2,"(7800)","[n_1,n_2,n_4,n_5,n_6,n_7,n_8,n_9]"
```

`(s, r, amt)` is a transfer, `(s)` is a balance query, `F(n_x)` fails a node and `R(n_x)` brings it back. The live-node list on a set's first row decides which nodes start the set up, and the lowest live node in each cluster becomes its leader. Operations within a set run one at a time, in order.

## Benchmarks and resharding

`RunBenchmark` runs a generated workload through the same client: a SmallBank-style mix (60% reads, 30% of writes cross-shard), YCSB-like workloads A, B, C, D and F, or your own read ratio, cross-shard ratio and Zipf skew. It reports duration, successful and failed reads and writes, throughput and success rate. Under heavy skew the hot accounts run dry fast, so expect a good share of writes to be rejected for insufficient balance.

`PrintReshard` treats the transfers seen so far as a graph of accounts used together and greedily proposes moving accounts so that frequently paired ones share a cluster, then reports how many cross-shard pairs would remain. It can simulate the moves but does not migrate data in the running system.

## Code layout

```
node/         Multi-Paxos, leader election, log recovery and the 2PC handlers
client/       client that finds each cluster's leader and retries during elections
benchmark/    SmallBank and YCSB-style workload generators
reshard/      resharding analysis
proto/        gRPC service and message definitions
main.go       starts the nine nodes and runs the test file
tests/        sample test file
```

## Limitations

Locks live only on the cluster leader, so a leader change in the middle of a transaction drops them. There is no termination protocol for 2PC, which means a coordinator that fails between prepare and commit leaves its transaction in doubt. A leader that has lost its majority still answers balance queries from its own copy. The topology is fixed at three clusters of three nodes.
