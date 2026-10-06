package node

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	pb "2pc-DarkBrewww/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// participantGate makes sure a 2PC message is handled by this cluster's leader.
// A follower forwards it once to the node it believes leads; anything else gets a
// retryable Unavailable error so the coordinator tries another node.
// Returns (forward target, error). With both nil, this node handles the message.
func (n *Node) participantGate(ctx context.Context) (pb.NodeServiceClient, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.Failed {
		return nil, status.Error(codes.Unavailable, "node is failed")
	}
	if n.isReadyLeader() {
		return nil, nil
	}
	leaderID := n.CurrentLeader
	if isForwarded(ctx) || leaderID <= 0 || leaderID == n.ID {
		return nil, errNotLeader
	}
	if peer, ok := n.Peers[leaderID]; ok {
		return peer, nil
	}
	return nil, errNotLeader
}

// Send2PCPrepare handles prepare phase from coordinator
func (n *Node) Send2PCPrepare(ctx context.Context, msg *pb.TwoPCPrepare) (*pb.TwoPCPrepared, error) {
	if fwd, err := n.participantGate(ctx); err != nil {
		return nil, err
	} else if fwd != nil {
		return fwd.Send2PCPrepare(forwardCtx(ctx), msg)
	}

	if msg.Transaction == nil || msg.OriginalRequest == nil {
		return &pb.TwoPCPrepared{RequestId: msg.RequestId, Success: false}, nil
	}

	n.mu.Lock()
	receiver := msg.Transaction.Receiver

	// Duplicate prepare for a transaction already prepared here
	if _, ok := n.preparedTxns[msg.RequestId]; ok {
		n.mu.Unlock()
		return &pb.TwoPCPrepared{Transaction: msg.Transaction, RequestId: msg.RequestId, Success: true}, nil
	}

	// Check if receiver is locked
	if n.Locks[receiver] {
		n.mu.Unlock()

		// Run Paxos to replicate abort record
		n.runPaxosConsensus(ctx, msg.OriginalRequest, "A")

		return &pb.TwoPCPrepared{
			Transaction: msg.Transaction,
			RequestId:   msg.RequestId,
			Success:     false,
		}, nil
	}

	// Lock receiver
	n.Locks[receiver] = true
	n.mu.Unlock()

	// Run Paxos for prepare phase (credits the receiver and writes the WAL on every replica)
	success := n.runPaxosConsensus(ctx, msg.OriginalRequest, "P")

	n.mu.Lock()
	if !success {
		delete(n.Locks, receiver)
		n.mu.Unlock()
		return &pb.TwoPCPrepared{Transaction: msg.Transaction, RequestId: msg.RequestId, Success: false}, nil
	}
	n.preparedTxns[msg.RequestId] = receiver
	n.mu.Unlock()

	return &pb.TwoPCPrepared{
		Transaction: msg.Transaction,
		RequestId:   msg.RequestId,
		Success:     true,
	}, nil
}

// releasePrepared drops the lock this participant took for a prepared transaction.
func (n *Node) releasePrepared(reqID string, receiver int32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if item, ok := n.preparedTxns[reqID]; ok {
		delete(n.Locks, item)
		delete(n.preparedTxns, reqID)
	} else {
		delete(n.Locks, receiver)
	}
}

// Send2PCCommit handles commit phase from coordinator
func (n *Node) Send2PCCommit(ctx context.Context, msg *pb.TwoPCCommit) (*pb.TwoPCAck, error) {
	if fwd, err := n.participantGate(ctx); err != nil {
		return nil, err
	} else if fwd != nil {
		return fwd.Send2PCCommit(forwardCtx(ctx), msg)
	}
	if msg.Transaction == nil {
		return &pb.TwoPCAck{RequestId: msg.RequestId, Success: false}, nil
	}

	// Already committed here (coordinator retry)
	n.mu.RLock()
	outcome, decided := n.clientLastReply[msg.RequestId]
	n.mu.RUnlock()
	if decided {
		return &pb.TwoPCAck{Transaction: msg.Transaction, RequestId: msg.RequestId, Success: outcome.Success}, nil
	}

	// Create a request for consensus
	req := &pb.ClientRequest{
		Type:        pb.ClientRequest_TRANSFER,
		RequestId:   msg.RequestId,
		Transaction: msg.Transaction,
	}

	// Run Paxos for commit phase (clears the WAL on every replica)
	ok := n.runPaxosConsensus(ctx, req, "C")
	if ok {
		n.releasePrepared(msg.RequestId, msg.Transaction.Receiver)
	}

	return &pb.TwoPCAck{
		Transaction: msg.Transaction,
		RequestId:   msg.RequestId,
		Success:     ok,
	}, nil
}

// Send2PCAbort handles abort phase from coordinator
func (n *Node) Send2PCAbort(ctx context.Context, msg *pb.TwoPCAbort) (*pb.TwoPCAck, error) {
	if fwd, err := n.participantGate(ctx); err != nil {
		return nil, err
	} else if fwd != nil {
		return fwd.Send2PCAbort(forwardCtx(ctx), msg)
	}
	if msg.Transaction == nil {
		return &pb.TwoPCAck{RequestId: msg.RequestId, Success: false}, nil
	}

	n.mu.RLock()
	_, prepared := n.preparedTxns[msg.RequestId]
	_, hasWAL := n.WAL[msg.RequestId]
	_, decided := n.clientLastReply[msg.RequestId]
	n.mu.RUnlock()

	// Nothing prepared here (we refused, or already aborted): nothing to undo
	if decided || (!prepared && !hasWAL) {
		n.releasePrepared(msg.RequestId, msg.Transaction.Receiver)
		return &pb.TwoPCAck{Transaction: msg.Transaction, RequestId: msg.RequestId, Success: true}, nil
	}

	// Create a request for consensus
	req := &pb.ClientRequest{
		Type:        pb.ClientRequest_TRANSFER,
		RequestId:   msg.RequestId,
		Transaction: msg.Transaction,
	}

	// Run Paxos for abort phase; executing it undoes the prepare on every replica via the WAL
	ok := n.runPaxosConsensus(ctx, req, "A")
	if ok {
		n.releasePrepared(msg.RequestId, msg.Transaction.Receiver)
	}

	return &pb.TwoPCAck{
		Transaction: msg.Transaction,
		RequestId:   msg.RequestId,
		Success:     ok,
	}, nil
}

// Leader election related functions
func (n *Node) SendPrepare(ctx context.Context, msg *pb.PrepareMessage) (*pb.PromiseMessage, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Failed || !n.epochOK(ctx) {
		return nil, status.Error(codes.Unavailable, "node is failed")
	}

	// Promise only a strictly higher ballot. Otherwise tell the candidate the ballot
	// we already promised so it can pick a higher one next time.
	if msg.BallotNumber <= n.BallotNumber {
		return &pb.PromiseMessage{BallotNumber: n.BallotNumber, SenderId: n.ID}, nil
	}
	n.BallotNumber = msg.BallotNumber

	// A leader that promises a higher ballot can no longer lead
	if msg.SenderId != n.ID {
		n.becomeFollower(-1) // leader unknown until the NewView; also restarts our timer
	}

	// Collect accepted values (committed entries are flagged Committed)
	acceptedValues := make([]*pb.LogEntry, 0, len(n.AcceptedValues))
	for _, entry := range n.AcceptedValues {
		if entry.Request == nil || entry.Phase == PhaseHeartbeat {
			continue
		}
		acceptedValues = append(acceptedValues, entry)
	}

	return &pb.PromiseMessage{
		BallotNumber:   msg.BallotNumber,
		SenderId:       n.ID,
		AcceptedValues: acceptedValues,
	}, nil
}

func (n *Node) SendNewView(ctx context.Context, msg *pb.NewViewMessage) (*pb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Failed || !n.epochOK(ctx) {
		return &pb.Empty{}, nil
	}
	if msg.BallotNumber < n.BallotNumber {
		return &pb.Empty{}, nil // stale view
	}

	n.BallotNumber = msg.BallotNumber
	if msg.NewLeaderId != n.ID {
		n.becomeFollower(msg.NewLeaderId)
	}
	// Record view change
	n.ViewChanges = append(n.ViewChanges, msg)

	log.Printf("Node %d: New leader is %d with ballot %d", n.ID, msg.NewLeaderId, msg.BallotNumber)

	return &pb.Empty{}, nil
}

// StartLeaderElection initiates leader election (used by the harness to put the
// lowest live node of each cluster in charge at the start of a test set)
func (n *Node) StartLeaderElection(ctx context.Context) {
	n.runElection(true)
}

// runElection runs one election. With retryStale, a candidate whose ballot is only
// out of date (it was down while others moved on) retries once with a higher one.
func (n *Node) runElection(retryStale bool) {
	n.mu.Lock()
	if n.Failed || n.electing {
		n.mu.Unlock()
		return
	}
	n.electing = true
	epoch := n.epoch
	if n.State == "LEADER" || n.IsLeader {
		n.leaderGen++ // stop the old term's heartbeats
	}
	n.State = "CANDIDATE"
	n.IsLeader = false
	n.mu.Unlock()

	defer func() {
		n.mu.Lock()
		n.electing = false
		n.mu.Unlock()
	}()

	var (
		ballotNum int32
		promises  []*pb.PromiseMessage
	)
	// A node that was down may hold a stale ballot; if live peers only refused
	// because of that, retry once with a ballot above the highest one they reported.
	for attempt := 1; ; attempt++ {
		n.mu.Lock()
		if n.Failed || n.epoch != epoch || n.State != "CANDIDATE" {
			n.mu.Unlock()
			return
		}
		n.BallotNumber = n.nextBallot()
		ballotNum = n.BallotNumber
		n.mu.Unlock()

		log.Printf("Node %d starting leader election with ballot %d", n.ID, ballotNum)

		prepareMsg := &pb.PrepareMessage{
			BallotNumber: ballotNum,
			SenderId:     n.ID,
		}

		// Send prepare to all peers in parallel
		var (
			wg         sync.WaitGroup
			pmu        sync.Mutex
			maxBallot  = ballotNum
			responders = 1 // self
		)
		promises = nil
		for _, peer := range n.Peers {
			wg.Add(1)
			go func(p pb.NodeServiceClient) {
				defer wg.Done()
				pCtx, cancel := context.WithTimeout(withEpoch(context.Background(), epoch), PrepareWait)
				resp, err := p.SendPrepare(pCtx, prepareMsg)
				cancel()
				if err != nil || resp == nil {
					return
				}
				pmu.Lock()
				defer pmu.Unlock()
				responders++
				if resp.BallotNumber == ballotNum {
					promises = append(promises, resp)
				} else if resp.BallotNumber > maxBallot {
					maxBallot = resp.BallotNumber
				}
			}(peer)
		}
		wg.Wait()

		n.mu.Lock()
		won := 1+len(promises) >= n.majority() &&
			!n.Failed && n.epoch == epoch && n.State == "CANDIDATE" && n.BallotNumber == ballotNum
		if won {
			break // keep holding the lock: becoming leader below
		}
		if maxBallot > n.BallotNumber {
			n.BallotNumber = maxBallot
		}
		staleBallot := maxBallot > ballotNum && responders >= n.majority()
		if retryStale && attempt < 2 && staleBallot && n.State == "CANDIDATE" && !n.Failed {
			n.mu.Unlock()
			continue
		}
		if n.State == "CANDIDATE" {
			// Lost or no majority: stay a follower and retry after another timeout
			n.becomeFollower(-1)
		}
		n.mu.Unlock()
		log.Printf("Node %d lost election with ballot %d (%d promises)", n.ID, ballotNum, 1+len(promises))
		return
	}

	// Became leader
	n.IsLeader = true
	n.CurrentLeader = n.ID
	n.State = "LEADER"
	n.Recovering = true // do not serve requests until the log is recovered
	n.leaderGen++
	gen := n.leaderGen
	if n.followerTimer != nil {
		n.followerTimer.Stop()
		n.followerTimer = nil
	}

	newViewMsg := &pb.NewViewMessage{
		BallotNumber: ballotNum,
		NewLeaderId:  n.ID,
		Timestamp:    time.Now().Unix(),
	}
	// Record own view change
	n.ViewChanges = append(n.ViewChanges, newViewMsg)
	n.mu.Unlock()

	// Send new-view to all nodes
	for _, peer := range n.Peers {
		go func(p pb.NodeServiceClient) {
			nvCtx, cancel := context.WithTimeout(withEpoch(context.Background(), epoch), time.Second)
			defer cancel()
			p.SendNewView(nvCtx, newViewMsg)
		}(peer)
	}

	log.Printf("Node %d became leader with ballot %d", n.ID, ballotNum)

	// Heartbeats for this term (new leaders never sent any before)
	go n.heartbeatLoop(gen)

	// Bring the log up to date from the promises before serving requests
	n.recoverLog(promises)

	n.mu.Lock()
	if n.leaderGen == gen {
		n.Recovering = false
	}
	n.mu.Unlock()
}

// recoverLog is run by a new leader. For every slot above its last executed one it
// adopts values already committed by some replica, re-proposes accepted-but-
// uncommitted values at the same sequence number with the new ballot, and fills
// holes with no-ops, so no replica ever executes different requests at one slot.
func (n *Node) recoverLog(promises []*pb.PromiseMessage) {
	n.mu.Lock()

	best := make(map[int32]*pb.LogEntry)
	consider := func(e *pb.LogEntry) {
		if e == nil || e.Request == nil || e.Phase == PhaseHeartbeat {
			return
		}
		cur, ok := best[e.SequenceNumber]
		switch {
		case !ok:
			best[e.SequenceNumber] = e
		case e.Committed && !cur.Committed:
			best[e.SequenceNumber] = e
		case !cur.Committed && !e.Committed && e.BallotNumber > cur.BallotNumber:
			best[e.SequenceNumber] = e
		}
	}
	for _, e := range n.AcceptedValues {
		consider(e)
	}
	for _, e := range n.CommittedLog {
		consider(e)
	}
	for _, p := range promises {
		for _, e := range p.AcceptedValues {
			consider(e)
		}
	}

	maxSeq := n.SequenceNumber
	if n.LastExecutedSeq > maxSeq {
		maxSeq = n.LastExecutedSeq
	}
	for seq := range best {
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	n.SequenceNumber = maxSeq

	type slot struct {
		seq   int32
		req   *pb.ClientRequest
		phase string
	}
	var toPropose []slot
	for seq := n.LastExecutedSeq + 1; seq <= maxSeq; seq++ {
		if _, ok := n.CommittedLog[seq]; ok {
			continue
		}
		e := best[seq]
		switch {
		case e != nil && e.Committed:
			committed := &pb.LogEntry{SequenceNumber: seq, BallotNumber: e.BallotNumber, Request: e.Request, Phase: e.Phase, Committed: true}
			n.AcceptedValues[seq] = committed
			n.CommittedLog[seq] = committed
		case e != nil:
			toPropose = append(toPropose, slot{seq, e.Request, e.Phase})
		default:
			toPropose = append(toPropose, slot{seq, &pb.ClientRequest{RequestId: fmt.Sprintf("noop-%d", seq)}, PhaseNoOp})
		}
	}
	n.mu.Unlock()

	if len(toPropose) > 0 {
		sort.Slice(toPropose, func(i, j int) bool { return toPropose[i].seq < toPropose[j].seq })
		log.Printf("[Node %d] Recovering %d uncommitted log slot(s) after election", n.ID, len(toPropose))
	}

	// Execute what is already known to be committed
	n.executeLog()

	for _, s := range toPropose {
		if !n.proposeAt(s.seq, s.req, s.phase) {
			log.Printf("[Node %d] Could not recover seq=%d (no majority)", n.ID, s.seq)
			return
		}
	}
}

// MonitorLeader starts a goroutine to monitor leader health
func (n *Node) MonitorLeader(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n.mu.RLock()
				isLeader := n.IsLeader
				failed := n.Failed
				n.mu.RUnlock()

				if !isLeader && !failed {
					// Check if we need to elect a new leader
					// This is a simplified version - in practice you'd have more sophisticated failure detection
					n.StartLeaderElection(ctx)
				}
			}
		}
	}()
}
