<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Design: safekeeper WAL replication

Status: **real network replication** — `NetworkReplicator` streams WAL to peer
safekeepers over TCP and commits on a real quorum, replacing the single-process
simulation. Wired into `aethel-safekeeper` via `--peer-addrs`.

## Roles

A safekeeper group is a small set of acceptors with one proposer (the leader
that compute streams to):

- **Leader** — receives `TYPE_APPEND` from compute, durably stores the bytes,
  and **replicates** them to the peers before acknowledging the commit position.
- **Acceptor (peer)** — receives `TYPE_REPLICATE` from the leader, durably
  stores + flushes the bytes, and replies with its flush LSN. It does **not**
  re-replicate — that one rule is what stops forwarding from looping.

The consensus arithmetic is unchanged: a WAL position is committed once a quorum
of members (leader + peers) has flushed at least that far
(`Consensus::commit_lsn`, the quorum-th largest flush position).

## Flow

```
 compute ──TYPE_APPEND──▶ leader ──store+flush──┐
                            │                    │  record own flush
                            ├──TYPE_REPLICATE──▶ peer 2 ──store+flush──▶ ack(flush_lsn)
                            └──TYPE_REPLICATE──▶ peer 3 ──store+flush──▶ ack(flush_lsn)
                            │
                       fold acks into consensus → commit_lsn = quorum-th flush
 compute ◀──AppendResponse(commit_lsn)──┘
```

`NetworkReplicator` keeps a connection open per peer and reconnects on failure.
A peer that is down simply contributes no ack, so a run still commits as long as
a quorum responds — verified by a test where one of three nodes is unreachable.

## Durability

The replicated bytes land in each peer's segmented, fsync'd WAL store exactly as
the leader's do. An end-to-end test runs three real safekeepers over sockets,
appends a run to the leader, asserts it commits only on quorum, and then reopens
each peer's on-disk store to confirm the bytes survived.

## Configuration

```
aethel-safekeeper --node-id 1 --members 1,2,3 \
  --peer-addrs 2=10.0.0.2:6500,3=10.0.0.3:6500
```

Without `--peer-addrs`, the safekeeper runs single-process with simulated
instantly-durable peers (dev/test).

## Leader election over the wire

A candidate stands for election by bumping the term and requesting votes from
its peers (`Safekeeper::run_election`, the `TYPE_VOTE` message). Each safekeeper
grants at most one vote per term (`Consensus::handle_vote_request`) and reports
its current term and flush position; the candidate wins once it has a quorum of
grants (its self-vote plus enough peers). A candidate standing in a term older
than one the peers have already voted in is denied — so two proposers can't both
win, preventing split-brain WAL.

Verified with three real safekeepers: a candidate wins a 2-of-3 quorum, and
loses when the peers have already voted in a higher term.

## Next

- **Catch-up / backfill** — a peer that was down should stream the WAL it missed
  on reconnect, not just resume at the current position.
- **Election triggers** — wire `run_election` to a startup/timeout policy (the
  mechanism is here; the *when* is a control-plane choice).
