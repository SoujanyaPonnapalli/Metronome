// Copyright 2015 The etcd Authors
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

package etcdserver

import (
	"expvar"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.etcd.io/etcd/pkg/v3/contention"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/metronome"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	// The max throughput of etcd will not exceed 100MB/s (100K * 1KB value).
	// Assuming the RTT is around 10ms, 1MB max size is large enough.
	maxSizePerMsg = 1 * 1024 * 1024
	// Never overflow the rafthttp buffer, which is 4096.
	// TODO: a better const?
	maxInflightMsgs = 4096 / 8
)

var (
	// protects raftStatus
	raftStatusMu sync.Mutex
	// indirection for expvar func interface
	// expvar panics when publishing duplicate name
	// expvar does not support remove a registered name
	// so only register a func that calls raftStatus
	// and change raftStatus as we need.
	raftStatus func() raft.Status
)

func init() {
	expvar.Publish("raft.status", expvar.Func(func() any {
		raftStatusMu.Lock()
		defer raftStatusMu.Unlock()
		if raftStatus == nil {
			return nil
		}
		return raftStatus()
	}))
}

// toApply contains entries, snapshot to be applied. Once
// an toApply is consumed, the entries will be persisted to
// raft storage concurrently; the application must read
// notifyc before assuming the raft messages are stable.
type toApply struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot
	// notifyc synchronizes etcd server applies with the raft node
	notifyc chan struct{}
	// raftAdvancedC notifies EtcdServer.apply that
	// 'raftLog.applied' has advanced by r.Advance
	// it should be used only when entries contain raftpb.EntryConfChange
	raftAdvancedC <-chan struct{}
}

type raftNode struct {
	lg *zap.Logger

	tickMu *sync.RWMutex
	// timestamp of the latest tick
	latestTickTs time.Time
	raftNodeConfig

	// a chan to send/receive snapshot
	msgSnapC chan raftpb.Message

	// a chan to send out apply
	applyc chan toApply

	// a chan to send out readState
	readStateC chan raft.ReadState

	// utility
	ticker *time.Ticker
	// contention detectors for raft heartbeat message
	td *contention.TimeoutDetector

	stopped chan struct{}
	done    chan struct{}
}

type raftNodeConfig struct {
	lg *zap.Logger

	// to check if msg receiver is removed from cluster
	isIDRemoved func(id uint64) bool
	raft.Node
	raftStorage *raft.MemoryStorage
	storage     serverstorage.Storage
	heartbeat   time.Duration // for logging
	// transport specifies the transport to send and receive msgs to members.
	// Sending messages MUST NOT block. It is okay to drop messages, since
	// clients should timeout and reissue their messages.
	// If transport is nil, server will panic.
	transport rafthttp.Transporter

	// localID is this node's raft ID. Used by the metronome scheme to
	// decide whether this node is in the persist-set for a given entry.
	// Zero when metronomeScheme is nil.
	localID uint64
	// metronomeScheme, if non-nil, marks metronome mode enabled and holds the
	// initial scheme. It filters which entries and snapshots this node
	// WAL-persists (HardState is always persisted). Used only as the
	// enabled-flag; runtime persist decisions go through currentScheme().
	metronomeScheme *metronome.Scheme

	// metronome is the shared oracle holding the live scheme pointer and this
	// node's durable watermark. It is created before raft.StartNode (so the
	// raft.Config.Metronome predicate is in place at startup) and shared with
	// the raft library's per-entry durability commit rule. Nil iff metronome
	// is disabled. currentScheme() and the durable watermark route through it.
	metronome *metronomeOracle

	// Work-stealing (Metronome §4.2). A follower tracks entries it
	// chose not to persist, alongside a timer reset on every
	// commit-index advance. If the commit stalls long enough while
	// skipped entries exist, we "steal" logging work from stragglers
	// by fsyncing the buffered entries ourselves and temporarily
	// logging every entry (not just our persist-set slot). This
	// keeps progress going when a peer in the K-of-N persist-set is
	// slow, at the cost of a small amount of extra logging on this
	// node.
	//
	// All fields below are read/written only on the raft Ready-loop
	// goroutine (recordCommitProgress + maybeTriggerWorkSteal), so the
	// steady-state path needs no lock. wsMu is taken only on the rare
	// work-steal FIRING path to pair the wsActiveUntil write with the
	// lock-free atomic mirror that the raft goroutine reads.
	wsMu          sync.Mutex
	wsLastCommit  uint64    // last seen committed index (for advance detection)
	wsLastAdvance time.Time // wall time of last commit-index advance
	wsActiveUntil time.Time // non-zero until this time => persist everything
	wsTimeout     time.Duration
	wsDuration    time.Duration

	// wsActiveUntilNanos mirrors wsActiveUntil as UnixNano so the
	// "are we in WS mode?" check can be lock-free on the hot path
	// (filterMetronomeEntries is called once per Ready). 0 means
	// inactive. Writes happen under wsMu *and* via atomic Store; reads
	// outside wsMu must go through Load.
	wsActiveUntilNanos atomic.Int64

	// filterBuf is a reusable backing slice for filterMetronomeEntries
	// to avoid per-Ready allocation. Single-goroutine access from the
	// Ready loop, so no synchronisation needed.
	filterBuf []raftpb.Entry
}

func newRaftNode(cfg raftNodeConfig) *raftNode {
	var lg raft.Logger
	if cfg.lg != nil {
		lg = NewRaftLoggerZap(cfg.lg)
	} else {
		lcfg := logutil.DefaultZapLoggerConfig
		var err error
		lg, err = NewRaftLogger(&lcfg)
		if err != nil {
			log.Fatalf("cannot create raft logger %v", err)
		}
	}
	raft.SetLogger(lg)
	r := &raftNode{
		lg:             cfg.lg,
		tickMu:         new(sync.RWMutex),
		raftNodeConfig: cfg,
		latestTickTs:   time.Now(),
		// set up contention detectors for raft heartbeat message.
		// expect to send a heartbeat within 2 heartbeat intervals.
		td:         contention.NewTimeoutDetector(2 * cfg.heartbeat),
		readStateC: make(chan raft.ReadState, 1),
		msgSnapC:   make(chan raftpb.Message, maxInFlightMsgSnap),
		applyc:     make(chan toApply),
		stopped:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	if r.heartbeat == 0 {
		r.ticker = &time.Ticker{}
	} else {
		r.ticker = time.NewTicker(r.heartbeat)
	}
	if r.metronome != nil {
		// Let the commit rule detect when this node is logging everything, so
		// it falls back to the standard rule during work-steal recovery.
		r.metronome.logEverything = r.inWorkStealMode
	}
	return r
}

// metronomeOracle holds the live metronome scheme and this node's durable
// watermark, shared between the etcd raftNode (which updates them) and the
// raft library's per-entry durability commit rule (which reads them via
// raft.Config.Metronome). It is constructed before raft.StartNode so the
// predicate is in place when the node starts. Implements raft.MetronomeOracle.
type metronomeOracle struct {
	scheme  atomic.Pointer[metronome.Scheme]
	durable atomic.Uint64 // this node's durable watermark index (post-fsync)
	// logEverything reports whether this node is in work-steal "log
	// everything" mode. Wired to raftNode.inWorkStealMode.
	logEverything func() bool
	// stallTicks / windowTicks express the work-steal timeout and persist-
	// everything duration in leader ticks, for the raft library's leader-driven
	// commit-stall fallback. Set at bootstrap from wsTimeout/wsDuration/TickMs.
	stallTicks  int
	windowTicks int
}

// ShouldPersist reports whether node id is assigned to persist index under the
// live scheme.
func (o *metronomeOracle) ShouldPersist(id, index uint64) bool {
	s := o.scheme.Load()
	return s != nil && s.ShouldPersist(id, index)
}

// Quorum returns T, the number of distinct durable copies required to commit
// (the live scheme's K).
func (o *metronomeOracle) Quorum() int {
	s := o.scheme.Load()
	if s == nil {
		return 0
	}
	return s.QuorumSize()
}

// SelfDurable returns this node's own durable watermark, advanced only after
// its storage fsync returns.
func (o *metronomeOracle) SelfDurable() uint64 { return o.durable.Load() }

// SelfLogEverything reports whether this node is in work-steal "log
// everything" mode (and the commit rule should fall back to standard raft).
func (o *metronomeOracle) SelfLogEverything() bool {
	return o.logEverything != nil && o.logEverything()
}

// StallTicks is the work-steal timeout in leader ticks.
func (o *metronomeOracle) StallTicks() int { return o.stallTicks }

// WindowTicks is the persist-everything duration in leader ticks.
func (o *metronomeOracle) WindowTicks() int { return o.windowTicks }

// currentScheme returns the active metronome scheme, or nil if metronome mode
// is disabled. Safe to call concurrently; the pointer is swapped atomically on
// membership changes.
func (r *raftNode) currentScheme() *metronome.Scheme {
	if r.metronome == nil {
		return nil
	}
	return r.metronome.scheme.Load()
}

// UpdateMetronomeScheme replaces the active scheme. Called from the
// apply loop after a ConfChange commits, so the scheme reflects the
// new membership. Passing nil is a no-op (keeps the existing scheme);
// the scheme cannot be disabled at runtime.
func (r *raftNode) UpdateMetronomeScheme(s *metronome.Scheme) {
	if s == nil || r.metronome == nil {
		return
	}
	r.metronome.scheme.Store(s)
	r.lg.Info("metronome scheme updated",
		zap.Int("cluster-size", s.NumNodes()),
		zap.Int("quorum-size", s.QuorumSize()),
	)
}

// attachMetronomeDurable stamps this node's durable watermark (index + term)
// and its out-of-stripe stolen copies onto every outgoing MsgAppResp via the
// Context field, so the leader's per-entry durability commit rule can count
// this node. No-op when metronome is disabled or nothing is durable yet.
// Called from the follower send path on the Ready loop goroutine; reads
// raftStorage.Term without a lock because that goroutine is the only writer of
// the durable watermark.
func (r *raftNode) attachMetronomeDurable(msgs []raftpb.Message) {
	if r.metronome == nil {
		return
	}
	w := r.metronome.durable.Load()
	if w == 0 {
		return
	}
	var term uint64
	if t, err := r.raftStorage.Term(w); err == nil {
		term = t
	}
	ctx := raft.EncodeDurableReport(raft.DurableReport{Index: w, Term: term})
	for i := range msgs {
		if msgs[i].Type == raftpb.MsgAppResp {
			msgs[i].Context = ctx
		}
	}
}

// raft.Node does not have locks in Raft package
func (r *raftNode) tick() {
	r.tickMu.Lock()
	r.Tick()
	r.latestTickTs = time.Now()
	r.tickMu.Unlock()
}

func (r *raftNode) getLatestTickTs() time.Time {
	r.tickMu.RLock()
	defer r.tickMu.RUnlock()
	return r.latestTickTs
}

// start prepares and starts raftNode in a new goroutine. It is no longer safe
// to modify the fields after it has been started.
func (r *raftNode) start(rh *raftReadyHandler) {
	internalTimeout := time.Second

	go func() {
		defer r.onStop()
		islead := false

		for {
			select {
			case <-r.ticker.C:
				r.tick()
			case rd := <-r.Ready():
				if rd.SoftState != nil {
					newLeader := rd.SoftState.Lead != raft.None && rh.getLead() != rd.SoftState.Lead
					if newLeader {
						leaderChanges.Inc()
					}

					if rd.SoftState.Lead == raft.None {
						hasLeader.Set(0)
					} else {
						hasLeader.Set(1)
					}

					rh.updateLead(rd.SoftState.Lead)
					islead = rd.RaftState == raft.StateLeader
					if islead {
						isLeader.Set(1)
					} else {
						isLeader.Set(0)
					}
					rh.updateLeadership(newLeader)
					r.td.Reset()
				}

				if len(rd.ReadStates) != 0 {
					select {
					case r.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
					case <-time.After(internalTimeout):
						r.lg.Warn("timed out sending read state", zap.Duration("timeout", internalTimeout))
					case <-r.stopped:
						return
					}
				}

				notifyc := make(chan struct{}, 1)
				raftAdvancedC := make(chan struct{}, 1)
				ap := toApply{
					entries:       rd.CommittedEntries,
					snapshot:      rd.Snapshot,
					notifyc:       notifyc,
					raftAdvancedC: raftAdvancedC,
				}

				updateCommittedIndex(&ap, rh)

				select {
				case r.applyc <- ap:
				case <-r.stopped:
					return
				}

				// the leader can write to its disk in parallel with replicating to the followers and then
				// writing to their disks.
				// For more details, check raft thesis 10.2.1
				if islead {
					// gofail: var raftBeforeLeaderSend struct{}
					r.transport.Send(r.processMessages(rd.Messages))
				}

				// Must save the snapshot file and WAL snapshot entry before saving any other entries or hardstate to
				// ensure that recovery after a snapshot restore is possible.
				if !raft.IsEmptySnap(rd.Snapshot) {
					if r.shouldPersistSnapshot(islead, rd.Snapshot.Metadata.Index) {
						// gofail: var raftBeforeSaveSnap struct{}
						if err := r.storage.SaveSnap(rd.Snapshot); err != nil {
							r.lg.Fatal("failed to save Raft snapshot", zap.Error(err))
						}
						// gofail: var raftAfterSaveSnap struct{}
					}
				}

				// Under metronome, filter entries so only nodes in the
				// persist-set for each entry index WAL-write it.
				// HardState is always passed through unchanged.
				entsToSave := r.entriesToPersist(rd.Entries, islead)
				if r.metronomeScheme != nil {
					r.recordCommitProgress(rd.HardState.Commit)
				}
				// gofail: var raftBeforeSave struct{}
				if err := r.storage.Save(rd.HardState, entsToSave); err != nil {
					r.lg.Fatal("failed to save Raft hard state and entries", zap.Error(err))
				}
				if !raft.IsEmptyHardState(rd.HardState) {
					proposalsCommitted.Set(float64(rd.HardState.Commit))
				}
				// gofail: var raftAfterSave struct{}

				if !raft.IsEmptySnap(rd.Snapshot) {
					// Force WAL to fsync its hard state before Release() releases
					// old data from the WAL. Otherwise could get an error like:
					// panic: tocommit(107) is out of range [lastIndex(84)]. Was the raft log corrupted, truncated, or lost?
					// See https://github.com/etcd-io/etcd/issues/10219 for more details.
					if err := r.storage.Sync(); err != nil {
						r.lg.Fatal("failed to sync Raft snapshot", zap.Error(err))
					}

					// etcdserver now claim the snapshot has been persisted onto the disk
					notifyc <- struct{}{}

					// gofail: var raftBeforeApplySnap struct{}
					r.raftStorage.ApplySnapshot(rd.Snapshot)
					r.lg.Info("applied incoming Raft snapshot", zap.Uint64("snapshot-index", rd.Snapshot.Metadata.Index))
					// gofail: var raftAfterApplySnap struct{}

					if err := r.storage.Release(rd.Snapshot); err != nil {
						r.lg.Fatal("failed to release Raft wal", zap.Error(err))
					}
					// gofail: var raftAfterWALRelease struct{}
				}

				// Metronome: this Ready's WAL writes (our persist-set entries
				// and any snapshot) have now fsynced, so advance this node's
				// durable watermark. Every index <= the highest appended index
				// that we were assigned to persist is on disk — we never skip
				// our own stripe — so the watermark is the highest appended (or
				// snapshot) index. The leader's per-entry durability commit rule
				// reads this (its own row via SelfDurable, followers' via the
				// report we attach to MsgAppResp). Advanced ONLY post-fsync so a
				// pre-fsync memory copy is never counted toward commit.
				if r.metronome != nil {
					var w uint64
					if n := len(rd.Entries); n > 0 {
						w = rd.Entries[n-1].Index
					}
					if !raft.IsEmptySnap(rd.Snapshot) && rd.Snapshot.Metadata.Index > w {
						w = rd.Snapshot.Metadata.Index
					}
					if w > r.metronome.durable.Load() {
						r.metronome.durable.Store(w)
					}
				}

				r.raftStorage.Append(rd.Entries)

				confChanged := false
				for _, ent := range rd.CommittedEntries {
					if ent.Type == raftpb.EntryConfChange {
						confChanged = true
						break
					}
				}

				if !islead {
					// finish processing incoming messages before we signal notifyc chan
					msgs := r.processMessages(rd.Messages)

					// Metronome: piggyback this node's durable watermark on each
					// outgoing MsgAppResp (in Context) so the leader's per-entry
					// durability commit rule can count us. Match still carries the
					// in-memory ack for flow control; durability rides separately.
					r.attachMetronomeDurable(msgs)

					// now unblocks 'applyAll' that waits on Raft log disk writes before triggering snapshots
					notifyc <- struct{}{}

					// Candidate or follower needs to wait for all pending configuration
					// changes to be applied before sending messages.
					// Otherwise we might incorrectly count votes (e.g. votes from removed members).
					// Also slow machine's follower raft-layer could proceed to become the leader
					// on its own single-node cluster, before toApply-layer applies the config change.
					// We simply wait for ALL pending entries to be applied for now.
					// We might improve this later on if it causes unnecessary long blocking issues.

					if confChanged {
						// blocks until 'applyAll' calls 'applyWait.Trigger'
						// to be in sync with scheduled config-change job
						// (assume notifyc has cap of 1)
						select {
						case notifyc <- struct{}{}:
						case <-r.stopped:
							return
						}
					}

					// gofail: var raftBeforeFollowerSend struct{}
					r.transport.Send(msgs)
				} else {
					// leader already processed 'MsgSnap' and signaled
					notifyc <- struct{}{}
				}

				// gofail: var raftBeforeAdvance struct{}
				r.Advance()

				if confChanged {
					// notify etcdserver that raft has already been notified or advanced.
					raftAdvancedC <- struct{}{}
				}

				// Metronome §4.2: cheap stall-check after each Ready.
				// No-op unless metronome is enabled and this node is sitting
				// on skipped entries while the committed index has stalled.
				// The LEADER participates too: under the per-entry durability
				// commit rule a committed index needs f+1 on-disk copies, so
				// when a persister is down the leader must also steal (log the
				// entries it skipped) to supply a durable copy for indices
				// whose persist-set excluded it — otherwise commit deadlocks
				// on those indices with no node willing to persist them.
				if r.metronomeScheme != nil {
					r.maybeTriggerWorkSteal()
				}
			case <-r.stopped:
				return
			}
		}
	}()
}

func updateCommittedIndex(ap *toApply, rh *raftReadyHandler) {
	var ci uint64
	if len(ap.entries) != 0 {
		ci = ap.entries[len(ap.entries)-1].Index
	}
	if ap.snapshot.Metadata.Index > ci {
		ci = ap.snapshot.Metadata.Index
	}
	if ci != 0 {
		rh.updateCommittedIndex(ci)
	}
}

func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if r.isIDRemoved(ms[i].To) {
			ms[i].To = 0
			continue
		}

		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {
			// There are two separate data store: the store for v2, and the KV for v3.
			// The msgSnap only contains the most recent snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store snapshot and KV snapshot.
			select {
			case r.msgSnapC <- ms[i]:
			default:
				// drop msgSnap if the inflight chan if full.
			}
			ms[i].To = 0
		}
		if ms[i].Type == raftpb.MsgHeartbeat {
			ok, exceed := r.td.Observe(ms[i].To)
			if !ok {
				// TODO: limit request rate.
				r.lg.Warn(
					"leader failed to send out heartbeat on time; took too long, leader is overloaded likely from slow disk",
					zap.String("to", fmt.Sprintf("%x", ms[i].To)),
					zap.Duration("heartbeat-interval", r.heartbeat),
					zap.Duration("expected-duration", 2*r.heartbeat),
					zap.Duration("exceeded-duration", exceed),
				)
				heartbeatSendFailures.Inc()
			}
		}
	}
	return ms
}

// shouldPersistSnapshot decides whether this node should WAL-persist
// the raft snapshot at the given index. Every node (leader included)
// persists only if it is in the snapshot's persist-set; when the scheme
// is disabled, all nodes persist.
//
// `islead` is retained in the signature for future role-aware policy
// but is intentionally unused today.
func (r *raftNode) shouldPersistSnapshot(_ bool, snapshotIndex uint64) bool {
	if r.metronomeScheme == nil {
		return true
	}
	scheme := r.currentScheme()
	if scheme == nil {
		return true
	}
	return scheme.ShouldPersist(r.localID, snapshotIndex)
}

// entriesToPersist decides which entries from a single Ready should be
// fsynced on this node. It is the single seam the Ready loop consults
// before calling storage.Save.
//
// In leader-shuffles metronome, every node (leader included) persists
// only entries in its rotating persist-set. The leader's own commits
// are acked from memory; followers' acks already work that way under
// the canonical scheme. K-of-N copies on disk remain the safety
// invariant, identical to the canonical scheme but distributed
// uniformly across all N nodes.
//
// `islead` is retained in the signature for future role-aware policy
// (e.g. an optional "leader always persists ConfChange" override) but
// is intentionally unused today — see the leader_shuffles_test.go
// suite for the contract.
func (r *raftNode) entriesToPersist(ents []raftpb.Entry, _ bool) []raftpb.Entry {
	if r.metronomeScheme == nil {
		return ents
	}
	return r.filterMetronomeEntries(ents)
}

// filterMetronomeEntries returns the subset of entries that this
// node should WAL-persist under the active metronome scheme.
// Configuration-change entries (EntryConfChange, EntryConfChangeV2) are
// always kept: membership transitions must be durable on every node so
// the scheme can be reconstructed post-restart. When the node is in
// work-stealing mode (after recent straggler-driven timeout), ALL
// entries are kept regardless of the scheme — strictly stronger than
// the scheme's K-of-N guarantee, so safety is preserved.
//
// Hot path. The Ready loop calls this once per iteration with
// (typically) 1–50 entries. We:
//   - early-return when nothing to filter, before any atomic loads;
//   - read the work-steal flag lock-free via wsActiveUntilNanos;
//   - reuse a node-local buffer (filterBuf) for the output to avoid
//     per-Ready allocation.
func (r *raftNode) filterMetronomeEntries(ents []raftpb.Entry) []raftpb.Entry {
	if len(ents) == 0 {
		return ents
	}
	if r.inWorkStealMode() {
		return ents
	}
	scheme := r.currentScheme()
	if scheme == nil {
		return ents // scheme not set yet; fall back to persist-all
	}
	// Reuse the per-node buffer; reset length, keep capacity.
	out := r.filterBuf[:0]
	if cap(out) < len(ents) {
		out = make([]raftpb.Entry, 0, len(ents))
	}
	// Order matters for branch prediction. ShouldPersist is the common
	// short-circuit (K/N of entries are kept) and is now O(1), so check
	// it first; ConfChange-fallthrough is rare. With this ordering the
	// common kept-entry path is 1 op instead of 3.
	localID := r.localID
	for i := range ents {
		e := &ents[i]
		keep := scheme.ShouldPersist(localID, e.Index) ||
			e.Type == raftpb.EntryConfChange ||
			e.Type == raftpb.EntryConfChangeV2
		if keep {
			out = append(out, *e)
		}
	}
	r.filterBuf = out
	return out
}

// ---- Work stealing (Metronome §4.2) ---------------------------------

// setWSActiveUntil updates both wsActiveUntil and its atomic mirror
// wsActiveUntilNanos in lockstep so the lock-free inWorkStealMode read
// stays consistent. Caller must hold wsMu (or otherwise have exclusive
// access; tests use this during setup before the Ready loop runs).
func (r *raftNode) setWSActiveUntil(t time.Time) {
	r.wsActiveUntil = t
	if t.IsZero() {
		r.wsActiveUntilNanos.Store(0)
	} else {
		r.wsActiveUntilNanos.Store(t.UnixNano())
	}
}

// inWorkStealMode returns true if this node is currently in the
// "log everything" window triggered by a recent straggler timeout.
//
// Lock-free fast path: reads wsActiveUntilNanos atomically. The
// authoritative time.Time wsActiveUntil is still kept under wsMu for
// the slow-path writers (maybeTriggerWorkSteal), but readers on the
// hot path avoid the mutex.
func (r *raftNode) inWorkStealMode() bool {
	if r.metronomeScheme == nil {
		return false
	}
	nanos := r.wsActiveUntilNanos.Load()
	if nanos == 0 {
		return false
	}
	return time.Now().UnixNano() < nanos
}

// recordReadySkipped bookkeeps which entry indices from `allEnts` were
// filtered out of `kept` (i.e., NOT fsynced this Ready), and updates
// the commit-advance timestamp when HardState.Commit moves forward.
// Safe to call unconditionally; no-op when metronome is disabled.
// recordCommitProgress tracks the global committed index for work-steal
// stall detection. It is the ENTIRE steady-state work-steal cost: a single
// comparison, and on a commit advance two field writes. There is no
// per-Ready skip buffer, lock, or O(len) walk — the set of skipped entries
// to flush is reconstructed lazily from the scheme only if work-steal
// actually fires (see maybeTriggerWorkSteal). Runs on the Ready-loop
// goroutine, the sole writer of these fields.
func (r *raftNode) recordCommitProgress(hsCommit uint64) {
	if r.metronomeScheme == nil {
		return
	}
	// Reset the stall timer whenever the committed index advances. If commit
	// keeps advancing the cluster is healthy; only a genuine stall (no
	// advance for wsTimeout while entries are pending) triggers work-steal.
	if hsCommit > r.wsLastCommit {
		r.wsLastCommit = hsCommit
		r.wsLastAdvance = time.Now()
	}
}

// maybeTriggerWorkSteal checks the stall condition and, if met,
// flushes the buffered skipped entries to WAL and enters the
// "log everything" window for wsDuration. Called from the Ready
// loop after each iteration — cheap in the common case (O(1) time
// check; no syscalls unless we actually steal).
func (r *raftNode) maybeTriggerWorkSteal() {
	if r.metronomeScheme == nil {
		return
	}
	r.wsMu.Lock()
	// Already stealing? Exit once the window is up.
	if !r.wsActiveUntil.IsZero() {
		if time.Now().Before(r.wsActiveUntil) {
			r.wsMu.Unlock()
			return
		}
		// Window elapsed — return to normal filtering.
		r.setWSActiveUntil(time.Time{})
	}
	// Fire on a COMMIT STALL with pending entries — not merely on having a
	// non-empty skip buffer. The leader must enter persist-everything even
	// when the index blocking commit is in ITS OWN stripe (so its skip buffer
	// is empty): only by switching to the standard commit rule (see
	// maybeCommit's SelfLogEverything fallback) can it unblock commit when a
	// persister is down. "Pending entries" = our log extends past the
	// committed index.
	last, lerr := r.raftStorage.LastIndex()
	if lerr != nil || last <= r.wsLastCommit {
		r.wsMu.Unlock()
		return
	}
	// Don't fire until we've seen at least one real commit advance.
	// Otherwise the pre-election / initial-replication startup window
	// counts against the timeout, and every follower would immediately
	// enter "log everything" mode the first time we restart, defeating
	// metronome's whole point.
	if r.wsLastCommit == 0 {
		r.wsMu.Unlock()
		return
	}
	// Has the committed index stalled long enough? Trigger is based purely on
	// the global commit index not advancing (not on which entry we hold) —
	// when it stalls past the timeout while we have buffered entries, log them.
	if time.Since(r.wsLastAdvance) < r.wsTimeout {
		r.wsMu.Unlock()
		return
	}

	// Prepare the steal: reconstruct the skipped indices to flush directly
	// from the scheme over the pending range (wsLastCommit, last] — the
	// entries this node did NOT persist. Doing it here (rare firing path)
	// instead of buffering every Ready keeps the steady-state path free.
	var toFlush []uint64
	if scheme := r.currentScheme(); scheme != nil {
		for idx := r.wsLastCommit + 1; idx <= last; idx++ {
			if !scheme.ShouldPersist(r.localID, idx) {
				toFlush = append(toFlush, idx)
			}
		}
	}
	jitter := time.Duration(int64(r.wsDuration) / 8) // ±12.5% jitter
	r.setWSActiveUntil(time.Now().Add(r.wsDuration + jitterDuration(jitter)))
	r.wsMu.Unlock()

	// Pull the entries from the in-memory raft log and fsync them.
	ents := r.collectInMemoryEntries(toFlush)
	if len(ents) > 0 {
		if err := r.storage.Save(raftpb.HardState{}, ents); err != nil {
			r.lg.Warn("metronome work-steal: Save failed", zap.Error(err))
		}
	}
	metronomeWorkStealsTriggered.Inc()
	metronomeWorkStealEntries.Add(float64(len(ents)))
	r.lg.Info("metronome work-steal fired",
		zap.Int("skipped-buffered", len(toFlush)),
		zap.Int("entries-flushed", len(ents)),
		zap.Duration("window", r.wsDuration),
	)
}

// collectInMemoryEntries returns the raft entries for the given
// indices by reading from raftStorage (in-memory). Indices that are
// below the compaction floor or above the last index are silently
// skipped; the caller treats this as "nothing to do."
func (r *raftNode) collectInMemoryEntries(indices []uint64) []raftpb.Entry {
	if len(indices) == 0 {
		return nil
	}
	first, err := r.raftStorage.FirstIndex()
	if err != nil {
		return nil
	}
	last, err := r.raftStorage.LastIndex()
	if err != nil || last == 0 {
		return nil
	}
	// Group into contiguous runs to minimize Entries() calls.
	out := make([]raftpb.Entry, 0, len(indices))
	i := 0
	for i < len(indices) {
		j := i + 1
		for j < len(indices) && indices[j] == indices[j-1]+1 {
			j++
		}
		lo := indices[i]
		hi := indices[j-1] + 1
		if lo < first {
			lo = first
		}
		if hi > last+1 {
			hi = last + 1
		}
		if lo < hi {
			ents, e := r.raftStorage.Entries(lo, hi, ^uint64(0))
			if e == nil {
				wanted := make(map[uint64]struct{}, j-i)
				for k := i; k < j; k++ {
					wanted[indices[k]] = struct{}{}
				}
				for k := range ents {
					if _, ok := wanted[ents[k].Index]; ok {
						out = append(out, ents[k])
					}
				}
			}
		}
		i = j
	}
	return out
}

// jitterDuration returns a deterministic-enough pseudo-random offset
// in [-max, max]. We use time.Now() as the source so two concurrent
// recoverers don't trigger work-stealing synchronously.
func jitterDuration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	// Tiny xorshift on nanosecond of now; acceptable for jitter.
	n := uint64(time.Now().UnixNano())
	n ^= n >> 16
	n *= 0x9E3779B97F4A7C15
	n ^= n >> 33
	// Map to [-max, max].
	return time.Duration(int64(n&0xFFFF)%int64(2*max)) - max
}

func (r *raftNode) apply() chan toApply {
	return r.applyc
}

func (r *raftNode) stop() {
	select {
	case r.stopped <- struct{}{}:
		// Not already stopped, so trigger it
	case <-r.done:
		// Has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by start()
	<-r.done
}

func (r *raftNode) onStop() {
	r.Stop()
	r.ticker.Stop()
	r.transport.Stop()
	if err := r.storage.Close(); err != nil {
		r.lg.Panic("failed to close Raft storage", zap.Error(err))
	}
	close(r.done)
}

// for testing
func (r *raftNode) pauseSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Pause()
}

func (r *raftNode) resumeSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Resume()
}

// advanceTicks advances ticks of Raft node.
// This can be used for fast-forwarding election
// ticks in multi data-center deployments, thus
// speeding up election process.
func (r *raftNode) advanceTicks(ticks int) {
	for i := 0; i < ticks; i++ {
		r.tick()
	}
}

func (r *raftNode) ReadState() <-chan raft.ReadState {
	return r.readStateC
}
