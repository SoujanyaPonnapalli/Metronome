// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file implements the metronome per-entry durability-gated commit rule.
//
// Stock raft commits index j once a majority of voters report Match >= j,
// where Match is the highest *in-memory* index a replica has appended. Under
// the metronome scheme each replica WAL-persists only a rotating K-sized
// subset of entries, so "Match >= j" no longer implies "j is on that
// replica's disk" — a memory-only copy could be counted toward commit,
// committing an entry with fewer than f+1 durable copies.
//
// Design X closes that gap. The leader commits j only when at least
// Quorum() distinct replicas hold j DURABLY, determined from each replica's
// durable watermark against the deterministic persist-set:
//
//   - For a replica X assigned to persist j (ShouldPersist(X,j) == true): X
//     counts iff its durable watermark >= j. The watermark is gap-free over
//     X's assigned stripe, so watermark >= j implies j is on X's disk.
//   - For a replica X NOT assigned j: X counts only if it work-stole j and
//     reported (j, term) matching the leader's log (a real out-of-stripe
//     durable copy).
//
// Match is left untouched (it still drives flow control, leadership transfer,
// and snapshot decisions); the watermark lives in a separate Progress.Durable
// field and on the wire in MsgAppResp.Context. The computed index is handed to
// raftLog.maybeCommit, which re-applies raft's current-term commit gate, so
// safety (Figure 8) is preserved.

package raft

import "encoding/binary"

// MetronomeOracle supplies the leader with the deterministic persist-set and
// the durability threshold for the per-entry commit rule. When Config.Metronome
// is non-nil the rule is active. All methods run on the raft commit hot path,
// so they must be cheap.
type MetronomeOracle interface {
	// ShouldPersist reports whether replica id is assigned to WAL-persist the
	// entry at index (its persist-set membership for that index).
	ShouldPersist(id, index uint64) bool
	// Quorum is the number of distinct durable copies required to commit (T;
	// the metronome quorum size K, default f+1). Read live so a changed
	// --metronome-quorum-size or membership is reflected.
	Quorum() int
	// SelfDurable is this node's own durable watermark, used for the leader's
	// own row in the count (its self-ack never reaches the tracker). It must
	// only advance after this node's storage fsync returns.
	SelfDurable() uint64
	// SelfLogEverything reports whether this node is currently in work-steal
	// "log everything" mode. When true the per-entry persist-set durability
	// rule is the wrong tool — the cluster is fully replicating to recover
	// from a straggler, so the leader falls back to the standard commit rule.
	SelfLogEverything() bool
	// StallTicks is how many leader ticks the per-entry commit may stay stuck
	// (with entries pending) before the leader enters the standard-commit
	// fallback window — i.e. the work-steal timeout expressed in ticks.
	StallTicks() int
	// WindowTicks is how long (in leader ticks) the standard-commit fallback
	// window lasts once entered — i.e. the persist-everything duration in ticks.
	WindowTicks() int
}

// DurableReport is the metronome payload a follower piggybacks on its
// MsgAppResp via the (otherwise unused on that path) Context field: its
// durable watermark (highest index up to which its assigned stripe is fsynced).
type DurableReport struct {
	Index uint64 // durable watermark
	Term  uint64 // term of the entry at Index, as the follower has it
}

// EncodeDurableReport serializes r for MsgAppResp.Context. It returns nil for
// the zero report so vanilla/non-metronome acks stay empty.
func EncodeDurableReport(r DurableReport) []byte {
	if r.Index == 0 && r.Term == 0 {
		return nil
	}
	buf := make([]byte, 0, 16)
	var tmp [binary.MaxVarintLen64]byte
	put := func(v uint64) {
		buf = append(buf, tmp[:binary.PutUvarint(tmp[:], v)]...)
	}
	put(r.Index)
	put(r.Term)
	return buf
}

// DecodeDurableReport parses a Context payload. ok is false for empty or
// malformed input; callers treat that as "no report" and never fail.
func DecodeDurableReport(b []byte) (rep DurableReport, ok bool) {
	if len(b) == 0 {
		return DurableReport{}, false
	}
	off := 0
	get := func() (uint64, bool) {
		v, n := binary.Uvarint(b[off:])
		if n <= 0 {
			return 0, false
		}
		off += n
		return v, true
	}
	if rep.Index, ok = get(); !ok {
		return DurableReport{}, false
	}
	if rep.Term, ok = get(); !ok {
		return DurableReport{}, false
	}
	return rep, true
}

// ingestDurableReport applies a follower's MsgAppResp durability payload by
// advancing that follower's durable watermark. It returns true if the
// watermark advanced, so the caller recomputes commit even when Match did not
// move (e.g. a straggler catching up on persistence without new appends).
// No-op when metronome is disabled or the report is empty.
func (r *raft) ingestDurableReport(from uint64, ctx []byte) (advanced bool) {
	if r.metronome == nil {
		return false
	}
	rep, ok := DecodeDurableReport(ctx)
	if !ok {
		return false
	}
	if pr := r.trk.Progress[from]; pr != nil {
		advanced = pr.MaybeUpdateDurable(rep.Index, rep.Term)
	}
	return advanced
}

// metronomeCommitted returns the highest index forming a contiguous durable
// prefix above raftLog.committed under the per-entry rule. The result is fed to
// raftLog.maybeCommit (current-term gate). O(N) to validate watermarks once,
// then O(N) per index advanced with short-circuit — no per-entry allocation.
func (r *raft) metronomeCommitted() uint64 {
	committed := r.raftLog.committed
	last := r.raftLog.lastIndex()
	if last <= committed {
		return committed
	}
	T := r.metronome.Quorum()
	if T <= 0 {
		T = 1
	}

	// Validate each voter's durable watermark once. The leader trusts its own
	// SelfDurable(); a follower's Durable is trusted only if the term it
	// reported still matches the leader's log there (otherwise its on-disk
	// prefix is stale/overwritten and must not count). vd defaults to 0
	// (under-count, never over-count). The map is reused across calls (this
	// runs on every MsgAppResp) — cleared in place, not reallocated per call.
	vd := r.metronomeVD
	if vd == nil {
		vd = make(map[uint64]uint64, len(r.trk.Progress))
		r.metronomeVD = vd
	} else {
		clear(vd)
	}
	for id, pr := range r.trk.Progress {
		if id == r.id {
			vd[id] = r.metronome.SelfDurable()
			continue
		}
		if pr.Durable == 0 {
			continue
		}
		if t, err := r.raftLog.term(pr.Durable); err == nil && t == pr.DurableTerm {
			vd[id] = pr.Durable
		}
	}
	// durablePersister reports whether replica id holds index j durably under
	// the per-entry rule: it must be in j's persist-set AND its durable
	// watermark must cover j. Out-of-persist-set copies (work-stolen during a
	// failure) are NOT counted here — that path is handled by falling back to
	// the standard commit rule while in persist-everything mode (see
	// maybeCommit). Not counting them keeps commit fully stalled at a wedged
	// index, which is what promptly triggers the work-steal timeout.
	durablePersister := func(id, j uint64) bool {
		return r.metronome.ShouldPersist(id, j) && vd[id] >= j
	}

	j := committed + 1
	for ; j <= last; j++ {
		if _, err := r.raftLog.term(j); err != nil {
			break
		}
		// Per-half majority of durable persisters (joint-config safety) AND
		// the metronome threshold T over distinct durable persisters.
		distinct := 0
		ok := true
		for half := range r.trk.Voters {
			cfg := r.trk.Voters[half]
			if len(cfg) == 0 {
				continue
			}
			cnt := 0
			for id := range cfg {
				if durablePersister(id, j) {
					cnt++
				}
			}
			if cnt < len(cfg)/2+1 {
				ok = false
				break
			}
			// For a single config (half 1 empty) this is the distinct count;
			// for a joint config the outgoing half bounds it conservatively.
			if distinct == 0 || cnt < distinct {
				distinct = cnt
			}
		}
		if !ok || distinct < T {
			break
		}
	}
	return j - 1
}
