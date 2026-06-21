// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! A lightweight Raft-style consensus state machine for the safekeeper quorum.
//!
//! The safekeepers form a small replication group. Two pieces of quorum logic
//! matter here, and both reduce to "a majority agrees":
//!
//! * **Commit LSN.** A WAL position is *committed* (durable, acknowledgeable to
//!   compute) once a quorum of safekeepers has flushed at least that far. Given
//!   each member's flush position, the commit LSN is the quorum-th largest of
//!   those positions — exactly Raft's `matchIndex` majority rule applied to
//!   byte offsets instead of log indices.
//! * **Leadership.** Terms (epochs) and votes elect a single proposer so that
//!   two compute nodes can't both append divergent WAL. A candidate becomes
//!   leader once a quorum grants it votes in its term.
//!
//! This module is pure (no I/O), so the quorum arithmetic is exhaustively
//! unit-tested; the safekeeper server drives it with real flush positions.

use std::collections::{HashMap, HashSet};

use common::Lsn;

/// A consensus member identifier.
pub type NodeId = u64;
/// A consensus term (epoch). Monotonically increasing.
pub type Term = u64;

/// The role this node currently plays in its term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Consensus state for one safekeeper node.
#[derive(Debug)]
pub struct Consensus {
    node_id: NodeId,
    /// All members of the group, including this node.
    members: Vec<NodeId>,
    /// Majority size: `members/2 + 1`.
    quorum: usize,

    term: Term,
    role: Role,
    voted_for: Option<NodeId>,
    votes: HashSet<NodeId>,

    /// Per-member durable flush position.
    flush: HashMap<NodeId, Lsn>,
}

impl Consensus {
    /// Build consensus state for `node_id` in a group of `members`
    /// (which must include `node_id`).
    pub fn new(node_id: NodeId, members: Vec<NodeId>) -> Self {
        assert!(members.contains(&node_id), "members must include this node");
        let quorum = members.len() / 2 + 1;
        let flush = members.iter().map(|&m| (m, Lsn::INVALID)).collect();
        Consensus {
            node_id,
            members,
            quorum,
            term: 0,
            role: Role::Follower,
            voted_for: None,
            votes: HashSet::new(),
            flush,
        }
    }

    /// This node's id.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
    /// Current term.
    pub fn term(&self) -> Term {
        self.term
    }
    /// Current role.
    pub fn role(&self) -> Role {
        self.role
    }
    /// Majority size for the group.
    pub fn quorum(&self) -> usize {
        self.quorum
    }
    /// Whether this node currently believes itself leader.
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// Record that `node` has durably flushed up to `lsn` (monotonic).
    pub fn record_flush(&mut self, node: NodeId, lsn: Lsn) {
        let slot = self.flush.entry(node).or_insert(Lsn::INVALID);
        if lsn > *slot {
            *slot = lsn;
        }
    }

    /// The quorum-committed LSN: the highest LSN flushed by at least `quorum`
    /// members. Equivalent to the quorum-th largest per-member flush position.
    pub fn commit_lsn(&self) -> Lsn {
        let mut positions: Vec<Lsn> =
            self.members.iter().map(|m| *self.flush.get(m).unwrap_or(&Lsn::INVALID)).collect();
        // Descending; the (quorum-1)-th element is flushed by `quorum` members.
        positions.sort_unstable_by(|a, b| b.cmp(a));
        positions.get(self.quorum - 1).copied().unwrap_or(Lsn::INVALID)
    }

    // ---- Leadership ----

    /// Adopt a newer term, reverting to follower and clearing the vote.
    fn observe_term(&mut self, term: Term) {
        if term > self.term {
            self.term = term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.votes.clear();
        }
    }

    /// Begin a new election: bump the term, become a candidate, and self-vote.
    /// Returns the new term for which votes should be requested.
    pub fn start_election(&mut self) -> Term {
        self.term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.node_id);
        self.votes.clear();
        self.votes.insert(self.node_id);
        self.maybe_become_leader();
        self.term
    }

    /// Handle a peer's vote request. Grants at most one vote per term.
    pub fn handle_vote_request(&mut self, term: Term, candidate: NodeId) -> bool {
        self.observe_term(term);
        if term == self.term && (self.voted_for.is_none() || self.voted_for == Some(candidate)) {
            self.voted_for = Some(candidate);
            true
        } else {
            false
        }
    }

    /// Record a vote granted to us by `from` in `term`.
    pub fn handle_vote_granted(&mut self, from: NodeId, term: Term) {
        if term == self.term && self.role == Role::Candidate {
            self.votes.insert(from);
            self.maybe_become_leader();
        }
    }

    fn maybe_become_leader(&mut self) {
        if self.role == Role::Candidate && self.votes.len() >= self.quorum {
            self.role = Role::Leader;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_is_majority() {
        assert_eq!(Consensus::new(1, vec![1]).quorum(), 1);
        assert_eq!(Consensus::new(1, vec![1, 2, 3]).quorum(), 2);
        assert_eq!(Consensus::new(1, vec![1, 2, 3, 4, 5]).quorum(), 3);
    }

    #[test]
    fn commit_lsn_is_quorum_th_largest() {
        let mut c = Consensus::new(1, vec![1, 2, 3]);
        assert_eq!(c.commit_lsn(), Lsn::INVALID);

        c.record_flush(1, Lsn(100));
        // Only one member has flushed: not committed yet (quorum=2).
        assert_eq!(c.commit_lsn(), Lsn::INVALID);

        c.record_flush(2, Lsn(80));
        // Two members at >=80 -> commit 80.
        assert_eq!(c.commit_lsn(), Lsn(80));

        c.record_flush(3, Lsn(50));
        // Sorted desc [100, 80, 50], quorum-th largest (index 1) = 80.
        assert_eq!(c.commit_lsn(), Lsn(80));

        c.record_flush(2, Lsn(100));
        // Now [100, 100, 50] -> commit 100.
        assert_eq!(c.commit_lsn(), Lsn(100));
    }

    #[test]
    fn record_flush_is_monotonic() {
        let mut c = Consensus::new(1, vec![1, 2, 3]);
        c.record_flush(1, Lsn(100));
        c.record_flush(1, Lsn(50)); // stale, ignored
        c.record_flush(2, Lsn(100));
        assert_eq!(c.commit_lsn(), Lsn(100));
    }

    #[test]
    fn single_node_commits_immediately() {
        let mut c = Consensus::new(1, vec![1]);
        c.record_flush(1, Lsn(42));
        assert_eq!(c.commit_lsn(), Lsn(42));
    }

    #[test]
    fn candidate_becomes_leader_on_quorum_votes() {
        let mut c = Consensus::new(1, vec![1, 2, 3]);
        let term = c.start_election();
        assert_eq!(term, 1);
        assert_eq!(c.role(), Role::Candidate); // self-vote only (1 < quorum 2)
        c.handle_vote_granted(2, term);
        assert!(c.is_leader());
        assert_eq!(c.role(), Role::Leader);
    }

    #[test]
    fn newer_term_demotes_to_follower() {
        let mut c = Consensus::new(1, vec![1, 2, 3]);
        c.start_election(); // term 1, candidate
                            // A peer requests a vote in a higher term -> we step down and may grant.
        let granted = c.handle_vote_request(5, 2);
        assert!(granted);
        assert_eq!(c.term(), 5);
        assert_eq!(c.role(), Role::Follower);
    }

    #[test]
    fn one_vote_per_term() {
        let mut c = Consensus::new(1, vec![1, 2, 3]);
        assert!(c.handle_vote_request(3, 2)); // grants to node 2
        assert!(!c.handle_vote_request(3, 3)); // already voted in term 3
    }
}
