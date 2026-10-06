package node

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	pb "2pc-DarkBrewww/proto"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// HeartbeatInterval is how often a leader pings its followers.
	HeartbeatInterval = 100 * time.Millisecond
	// A follower that hears nothing from the leader for FollowerTimeoutBase plus a
	// random jitter starts an election. It must be several heartbeat intervals long,
	// otherwise followers time out between heartbeats and leadership flaps.
	FollowerTimeoutBase   = 600 * time.Millisecond
	FollowerTimeoutJitter = 400 * time.Millisecond

	// AcceptWait bounds how long a leader waits for Accepted replies.
	AcceptWait = 500 * time.Millisecond
	// PrepareWait bounds how long a candidate waits for Promises.
	PrepareWait = 300 * time.Millisecond

	// PhaseHeartbeat marks heartbeat messages; PhaseNoOp fills log gaps after a leader change.
	PhaseHeartbeat = "HEARTBEAT"
	PhaseNoOp      = "NOOP"

	mdEpoch     = "x-epoch"
	mdForwarded = "x-forwarded"
)

type Node struct {
	pb.UnimplementedNodeServiceServer

	// Node identification
	ID        int32
	ClusterID int32
	Port      string

	// Cluster information
	ClusterNodes []string // Addresses of all nodes in this cluster

	// Leader election (Multi-Paxos)
	State         string // "FOLLOWER", "CANDIDATE", "LEADER"
	IsLeader      bool
	CurrentLeader int32
	BallotNumber  int32

	// Paxos state
	SequenceNumber  int32
	LastExecutedSeq int32
	AcceptedValues  map[int32]*pb.LogEntry   // seq -> log entry
	CommittedLog    map[int32]*pb.LogEntry   // seq -> committed entry
	AcceptedCount   map[int32]map[int32]bool // seq -> set of node IDs that accepted

	// Leader election state
	Promises    []*pb.PromiseMessage
	ViewChanges []*pb.NewViewMessage

	// Locking mechanism
	Locks map[int32]bool // data_item -> locked status

	// Write-Ahead Log for 2PC rollback
	WAL map[string][]*pb.WALEntry // request_id -> WAL entries

	// Database connection
	DB *sql.DB

	// Node state
	Failed     bool
	Recovering bool

	// Synchronization
	mu sync.RWMutex

	// Timers
	leaderTimeout time.Duration
	followerTimer *time.Timer

	// Peer connections (to other nodes in cluster)
	Peers map[int32]pb.NodeServiceClient

	// Cross-cluster connections (to leaders of other clusters)
	CoordinatorClients map[int32]pb.NodeServiceClient

	// Every node of each other cluster, so 2PC messages reach that cluster's
	// current leader even after its default leader fails.
	ClusterClients  map[int32][]pb.NodeServiceClient
	participantHint map[int32]int // index of the node that last answered for each cluster

	// Performance metrics
	txnStartTimes map[string]time.Time
	completedTxns int64
	totalLatency  time.Duration

	// Client request deduplication
	clientLastReply map[string]*pb.ClientResponse // final outcome of each executed request (replicated)
	seenRequests    map[string]int64

	PeerSyncStatus map[int32]bool // peer_id -> is_synced

	// 2PC participant bookkeeping: request_id -> data item locked for it
	preparedTxns map[string]int32

	bgCtx     context.Context
	leaderGen int64 // changes whenever this node starts or stops leading; stops stale heartbeat loops
	epoch     int64 // bumped by Flush; replication traffic from an older test set is dropped
	electing  bool
}

func NewNode(id, clusterID int32, port string, clusterNodes []string, dataRange [2]int32) (*Node, error) {
	// Initialize database
	dbPath := fmt.Sprintf("node_%d.db", id)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Create tables
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS balances (
			client_id INTEGER PRIMARY KEY,
			balance INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_client_id ON balances(client_id);
	`)
	if err != nil {
		return nil, err
	}

	// Initialize balances for this shard's data range (one transaction instead of 3000)
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	stmt, err := tx.Prepare("INSERT OR REPLACE INTO balances (client_id, balance) VALUES (?, ?)")
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	for i := dataRange[0]; i <= dataRange[1]; i++ {
		if _, err = stmt.Exec(i, 10); err != nil {
			stmt.Close()
			tx.Rollback()
			return nil, err
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	node := &Node{
		ID:                 id,
		ClusterID:          clusterID,
		Port:               port,
		ClusterNodes:       clusterNodes,
		State:              "FOLLOWER",
		IsLeader:           false,
		CurrentLeader:      -1,
		BallotNumber:       0,
		SequenceNumber:     0,
		LastExecutedSeq:    0,
		AcceptedValues:     make(map[int32]*pb.LogEntry),
		CommittedLog:       make(map[int32]*pb.LogEntry),
		AcceptedCount:      make(map[int32]map[int32]bool),
		Promises:           make([]*pb.PromiseMessage, 0),
		ViewChanges:        make([]*pb.NewViewMessage, 0),
		Locks:              make(map[int32]bool),
		WAL:                make(map[string][]*pb.WALEntry),
		DB:                 db,
		Failed:             false,
		leaderTimeout:      FollowerTimeoutBase,
		Peers:              make(map[int32]pb.NodeServiceClient),
		CoordinatorClients: make(map[int32]pb.NodeServiceClient),
		ClusterClients:     make(map[int32][]pb.NodeServiceClient),
		participantHint:    make(map[int32]int),
		txnStartTimes:      make(map[string]time.Time),
		clientLastReply:    make(map[string]*pb.ClientResponse),
		seenRequests:       make(map[string]int64),
		PeerSyncStatus:     make(map[int32]bool),
		preparedTxns:       make(map[string]int32),
		bgCtx:              context.Background(),
	}

	return node, nil
}

// --- Small helpers ---

func (n *Node) majority() int {
	return len(n.ClusterNodes)/2 + 1
}

// nextBallot returns a ballot above every ballot this node has seen. The node ID is
// the last digit, so two candidates can never use the same ballot (which let two
// nodes win the same election before).
func (n *Node) nextBallot() int32 {
	return (n.BallotNumber/10+1)*10 + n.ID
}

// withEpoch tags an outgoing replication message with this node's test-set epoch.
func withEpoch(ctx context.Context, epoch int64) context.Context {
	return metadata.AppendToOutgoingContext(ctx, mdEpoch, strconv.FormatInt(epoch, 10))
}

// epochOK reports whether an incoming message belongs to the current test set.
// MUST be called with the lock held.
func (n *Node) epochOK(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return true
	}
	vals := md.Get(mdEpoch)
	if len(vals) == 0 {
		return true
	}
	e, err := strconv.ParseInt(vals[0], 10, 64)
	return err != nil || e == n.epoch
}

func isForwarded(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	return ok && len(md.Get(mdForwarded)) > 0
}

func forwardCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, mdForwarded, "1")
}

var errNotLeader = status.Error(codes.Unavailable, "not the leader")

// becomeFollower demotes this node and points it at leaderID (-1 if unknown).
// MUST be called with the lock held.
func (n *Node) becomeFollower(leaderID int32) {
	if n.State == "LEADER" || n.IsLeader {
		log.Printf("[Node %d] Stepping down from LEADER", n.ID)
		n.leaderGen++ // stops the heartbeat loop of the old term
	}
	n.State = "FOLLOWER"
	n.IsLeader = false
	n.CurrentLeader = leaderID
	n.resetFollowerTimer()
}

// isReadyLeader reports whether this node may serve client and 2PC requests.
// MUST be called with the lock held.
func (n *Node) isReadyLeader() bool {
	return !n.Failed && n.IsLeader && n.State == "LEADER" && !n.Recovering
}

// StartFollowerTimer starts the follower timeout timer (called after leader is set)
func (n *Node) StartFollowerTimer() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Failed {
		return
	}

	if n.State == "FOLLOWER" {
		n.resetFollowerTimer()
	}
}

// StartHeartbeat sends periodic heartbeats to followers for the current leadership term
func (n *Node) StartHeartbeat(ctx context.Context) {
	n.mu.Lock()
	n.bgCtx = ctx
	gen := n.leaderGen
	n.mu.Unlock()
	go n.heartbeatLoop(gen)
}

func (n *Node) heartbeatLoop(gen int64) {
	n.mu.RLock()
	bgCtx := n.bgCtx
	n.mu.RUnlock()

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bgCtx.Done():
			return
		case <-ticker.C:
		}

		n.mu.Lock()
		if n.leaderGen != gen || !n.IsLeader || n.Failed || n.State != "LEADER" {
			n.mu.Unlock()
			return
		}
		ballotNum := n.BallotNumber
		leaderSeq := n.LastExecutedSeq
		epoch := n.epoch
		n.mu.Unlock()

		heartbeatMsg := &pb.AcceptMessage{
			BallotNumber:   ballotNum,
			SequenceNumber: leaderSeq,
			Phase:          PhaseHeartbeat,
			SenderId:       n.ID,
		}

		for peerID, peer := range n.Peers {
			// Skip peers we are already syncing
			n.mu.Lock()
			isSyncing := n.PeerSyncStatus[peerID]
			n.mu.Unlock()
			if isSyncing {
				continue
			}

			go func(pID int32, p pb.NodeServiceClient) {
				hbCtx, cancel := context.WithTimeout(withEpoch(bgCtx, epoch), 300*time.Millisecond)
				defer cancel()

				resp, err := p.SendAccept(hbCtx, heartbeatMsg)
				if err != nil || resp == nil {
					return
				}

				if !resp.Accepted {
					// The follower promised a higher ballot: a newer leader exists.
					if resp.BallotNumber > ballotNum {
						n.mu.Lock()
						if n.leaderGen == gen && n.State == "LEADER" {
							if resp.BallotNumber > n.BallotNumber {
								n.BallotNumber = resp.BallotNumber
							}
							n.becomeFollower(-1)
						}
						n.mu.Unlock()
					}
					return
				}

				// If the follower is behind, push the committed entries it is missing
				if resp.SequenceNumber < leaderSeq {
					n.mu.Lock()
					if n.PeerSyncStatus[pID] {
						n.mu.Unlock()
						return
					}
					n.PeerSyncStatus[pID] = true
					n.mu.Unlock()

					n.syncFollower(p, resp.SequenceNumber+1, leaderSeq, epoch)

					n.mu.Lock()
					n.PeerSyncStatus[pID] = false
					n.mu.Unlock()
				}
			}(peerID, peer)
		}
	}
}

// syncFollower pushes missing committed log entries to a stale follower
func (n *Node) syncFollower(peer pb.NodeServiceClient, startSeq, endSeq int32, epoch int64) {
	if startSeq > endSeq {
		return
	}
	log.Printf("[Node %d] Syncing follower from seq %d to %d", n.ID, startSeq, endSeq)

	for seq := startSeq; seq <= endSeq; seq++ {
		n.mu.RLock()
		entry, exists := n.CommittedLog[seq]
		stale := n.epoch != epoch
		n.mu.RUnlock()

		if stale || !exists {
			return
		}

		commitMsg := &pb.CommitMessage{
			SequenceNumber: entry.SequenceNumber,
			Request:        entry.Request,
			Phase:          entry.Phase,
		}

		syncCtx, cancel := context.WithTimeout(withEpoch(context.Background(), epoch), 2*time.Second)
		_, err := peer.SendCommit(syncCtx, commitMsg)
		cancel()
		if err != nil {
			return
		}
	}
}

// GetBalance retrieves balance from database
func (n *Node) GetBalance(ctx context.Context, req *pb.BalanceRequest) (*pb.BalanceResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	//if n.Failed {
	//	return nil, fmt.Errorf("node is failed")
	//}

	var balance int32
	err := n.DB.QueryRow("SELECT balance FROM balances WHERE client_id = ?", req.ClientId).Scan(&balance)
	if err != nil {
		return nil, err
	}

	return &pb.BalanceResponse{
		Balance: balance,
		NodeId:  n.ID,
	}, nil
}

// ProcessRequest handles client requests with proper Multi-Paxos
func (n *Node) ProcessRequest(ctx context.Context, req *pb.ClientRequest) (*pb.ClientResponse, error) {
	n.mu.Lock()
	if n.Failed {
		n.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "node is failed")
	}

	// If not leader, redirect (once: a forwarded request is never forwarded again,
	// which used to bounce forever between two nodes that each thought the other led)
	if !n.isReadyLeader() {
		leaderId := n.CurrentLeader
		state := n.State
		n.mu.Unlock()

		if isForwarded(ctx) || leaderId == n.ID || leaderId <= 0 {
			return nil, status.Errorf(codes.Unavailable, "no leader available (node %d state=%s)", n.ID, state)
		}

		log.Printf("[Node %d] Not leader (state=%s, leader=%d), redirecting", n.ID, state, leaderId)
		if peer, ok := n.Peers[leaderId]; ok {
			return peer.ProcessRequest(forwardCtx(ctx), req)
		}
		return nil, status.Errorf(codes.Unavailable, "no leader available (node %d state=%s)", n.ID, state)
	}

	// Duplicate of a request whose outcome is already in the replicated log
	if req.Type == pb.ClientRequest_TRANSFER {
		if lastReply, ok := n.clientLastReply[req.RequestId]; ok {
			n.mu.Unlock()
			log.Printf("[Node %d] Deduplication cache hit for %s", n.ID, req.RequestId)
			return lastReply, nil
		}
	}

	// Track start time for latency measurement
	n.txnStartTimes[req.RequestId] = time.Now()
	n.mu.Unlock()

	log.Printf("[Node %d] Leader processing request: %v", n.ID, req.Type)

	if req.Type == pb.ClientRequest_BALANCE {
		// Read-only transaction - leader responds directly
		balance, err := n.getBalanceFromDB(req.ClientId)
		if err != nil {
			return &pb.ClientResponse{
				Success: false,
				Message: err.Error(),
			}, nil
		}

		return &pb.ClientResponse{
			Success:   true,
			Balance:   balance,
			RequestId: req.RequestId,
		}, nil
	}

	if req.Transaction == nil {
		return &pb.ClientResponse{Success: false, Message: "Missing transaction", RequestId: req.RequestId}, nil
	}

	// Transfer transaction - requires consensus
	sender := req.Transaction.Sender
	receiver := req.Transaction.Receiver

	senderShard := n.getShardForClient(sender)
	receiverShard := n.getShardForClient(receiver)

	if receiverShard == -1 {
		return &pb.ClientResponse{Success: false, Message: "Invalid receiver", RequestId: req.RequestId}, nil
	}

	if senderShard == receiverShard && senderShard == n.ClusterID {
		// Intra-shard transaction
		return n.processIntraShardTransaction(ctx, req)
	} else if senderShard == n.ClusterID {
		// Cross-shard transaction - this node is coordinator
		return n.processCrossShardTransaction(ctx, req)
	} else {
		return &pb.ClientResponse{
			Success: false,
			Message: "Wrong coordinator",
		}, nil
	}
}

func (n *Node) processIntraShardTransaction(ctx context.Context, req *pb.ClientRequest) (*pb.ClientResponse, error) {
	n.mu.Lock()

	sender := req.Transaction.Sender
	receiver := req.Transaction.Receiver
	amount := req.Transaction.Amount

	// Check locks
	if n.Locks[sender] || n.Locks[receiver] {
		n.mu.Unlock()
		return &pb.ClientResponse{
			Success:   false,
			Message:   "Data items are locked",
			RequestId: req.RequestId,
		}, nil
	}

	// Check the balance before running consensus (the transfer used to be reported
	// as committed even when execution skipped it for insufficient funds)
	senderBalance, err := n.getBalanceFromDB(sender)
	if err != nil || senderBalance < amount {
		n.mu.Unlock()
		return &pb.ClientResponse{
			Success:   false,
			Message:   "Insufficient balance",
			RequestId: req.RequestId,
		}, nil
	}

	// Acquire locks
	n.Locks[sender] = true
	n.Locks[receiver] = true
	n.mu.Unlock()

	// Run Paxos consensus
	success := n.runPaxosConsensus(ctx, req, "")

	n.mu.Lock()
	// Release locks
	delete(n.Locks, sender)
	delete(n.Locks, receiver)
	stillLeader := n.isReadyLeader()
	n.mu.Unlock()

	if success {
		n.recordLatency(req.RequestId)
		return &pb.ClientResponse{
			Success:   true,
			Message:   "Transaction committed",
			RequestId: req.RequestId,
		}, nil
	}

	if !stillLeader {
		// Lost leadership mid-request: let the client retry at the new leader
		return nil, status.Error(codes.Unavailable, "leadership changed during consensus")
	}
	return &pb.ClientResponse{
		Success:   false,
		Message:   "Consensus failed (no majority in cluster)",
		RequestId: req.RequestId,
	}, nil
}

func (n *Node) processCrossShardTransaction(ctx context.Context, req *pb.ClientRequest) (*pb.ClientResponse, error) {
	sender := req.Transaction.Sender
	receiver := req.Transaction.Receiver
	amount := req.Transaction.Amount

	n.mu.Lock()

	// Check locks and balance on sender
	if n.Locks[sender] {
		n.mu.Unlock()
		return &pb.ClientResponse{
			Success:   false,
			Message:   "Sender is locked",
			RequestId: req.RequestId,
		}, nil
	}

	senderBalance, err := n.getBalanceFromDB(sender)
	if err != nil || senderBalance < amount {
		n.mu.Unlock()
		return &pb.ClientResponse{
			Success:   false,
			Message:   "Insufficient balance",
			RequestId: req.RequestId,
		}, nil
	}

	// Lock sender
	n.Locks[sender] = true
	n.mu.Unlock()

	releaseSender := func() {
		n.mu.Lock()
		delete(n.Locks, sender)
		n.mu.Unlock()
	}

	// Phase 1: Prepare phase for coordinator
	// Run Paxos with phase "P" (debits the sender and writes the WAL on every replica)
	if !n.runPaxosConsensus(ctx, req, "P") {
		releaseSender()
		return &pb.ClientResponse{
			Success:   false,
			Message:   "Coordinator prepare failed",
			RequestId: req.RequestId,
		}, nil
	}

	// Send prepare to the participant cluster's current leader
	participantShard := n.getShardForClient(receiver)
	prepareMsg := &pb.TwoPCPrepare{
		Transaction:     req.Transaction,
		OriginalRequest: req,
		RequestId:       req.RequestId,
	}

	var preparedResp *pb.TwoPCPrepared
	err = n.callParticipant(ctx, participantShard, 3*time.Second, func(c context.Context, p pb.NodeServiceClient) (bool, error) {
		resp, err := p.Send2PCPrepare(c, prepareMsg)
		if err != nil {
			return false, err
		}
		preparedResp = resp
		return true, nil
	})

	if err != nil || preparedResp == nil || !preparedResp.Success {
		// Abort: replicate the abort record locally (undoes the debit via WAL) and
		// tell the participant, which may have prepared before its reply was lost.
		log.Printf("[Node %d] Participant prepare failed or timeout: %v", n.ID, err)
		n.abort2PCTransaction(ctx, req)

		abortMsg := &pb.TwoPCAbort{Transaction: req.Transaction, RequestId: req.RequestId}
		n.callParticipant(ctx, participantShard, 1*time.Second, func(c context.Context, p pb.NodeServiceClient) (bool, error) {
			ack, err := p.Send2PCAbort(c, abortMsg)
			if err != nil {
				return false, err
			}
			return ack.Success, nil
		})

		n.recordLatency(req.RequestId)
		reason := "Participant prepare failed or timeout"
		if err == nil && preparedResp != nil {
			reason = "Participant aborted (receiver locked or no majority)"
		}
		return &pb.ClientResponse{
			Success:   false,
			Message:   "Transaction aborted: " + reason,
			RequestId: req.RequestId,
		}, nil
	}

	// Phase 2: Commit phase
	// Run Paxos for commit with phase "C"
	if !n.runPaxosConsensus(ctx, req, "C") {
		log.Printf("[Node %d] Warning: could not replicate commit record for %s", n.ID, req.RequestId)
	}

	// Send commit to participant with retry and wait for ACK
	commitMsg := &pb.TwoPCCommit{
		Transaction: req.Transaction,
		RequestId:   req.RequestId,
	}
	ackErr := n.callParticipant(ctx, participantShard, 3*time.Second, func(c context.Context, p pb.NodeServiceClient) (bool, error) {
		ack, err := p.Send2PCCommit(c, commitMsg)
		if err != nil {
			return false, err
		}
		return ack.Success, nil
	})
	if ackErr != nil {
		log.Printf("[Node %d] Warning: ACK not received from participant: %v", n.ID, ackErr)
	} else {
		log.Printf("[Node %d] Received ACK from participant", n.ID)
	}

	// Release lock and clear WAL
	n.mu.Lock()
	delete(n.Locks, sender)
	delete(n.WAL, req.RequestId)
	n.mu.Unlock()

	n.recordLatency(req.RequestId)
	return &pb.ClientResponse{
		Success:   true,
		Message:   "Cross-shard transaction committed",
		RequestId: req.RequestId,
	}, nil
}

// callParticipant sends a 2PC message to whichever node currently leads the given
// cluster. It tries the node that answered last time first, then the others, and
// keeps going until one of them answers or the window expires (covers elections).
// call returns (done, err): done=true stops the loop.
func (n *Node) callParticipant(ctx context.Context, clusterID int32, window time.Duration, call func(context.Context, pb.NodeServiceClient) (bool, error)) error {
	n.mu.RLock()
	clients := n.ClusterClients[clusterID]
	hint := n.participantHint[clusterID]
	n.mu.RUnlock()

	if len(clients) == 0 {
		if c := n.CoordinatorClients[clusterID]; c != nil {
			clients = []pb.NodeServiceClient{c}
		} else {
			return fmt.Errorf("no connection to cluster %d", clusterID)
		}
	}

	deadline := time.Now().Add(window)
	var lastErr error
	for {
		for i := 0; i < len(clients); i++ {
			idx := (hint + i) % len(clients)
			callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			done, err := call(callCtx, clients[idx])
			cancel()
			if err == nil {
				n.mu.Lock()
				n.participantHint[clusterID] = idx
				n.mu.Unlock()
				if done {
					return nil
				}
				lastErr = fmt.Errorf("participant cluster %d did not acknowledge", clusterID)
				continue
			}
			lastErr = err
		}
		if time.Now().After(deadline) {
			return lastErr
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Helper functions
func (n *Node) getBalanceFromDB(clientID int32) (int32, error) {
	var balance int32
	err := n.DB.QueryRow("SELECT balance FROM balances WHERE client_id = ?", clientID).Scan(&balance)
	return balance, err
}

func (n *Node) getShardForClient(clientID int32) int32 {
	if clientID >= 1 && clientID <= 3000 {
		return 1
	} else if clientID >= 3001 && clientID <= 6000 {
		return 2
	} else if clientID >= 6001 && clientID <= 9000 {
		return 3
	}
	return -1
}

func (n *Node) getLeaderForShard(shardID int32) pb.NodeServiceClient {
	// Default leader of the shard; callParticipant finds the current one
	return n.CoordinatorClients[shardID]
}

// runPaxosConsensus executes stable leader Multi-Paxos for the next sequence number.
func (n *Node) runPaxosConsensus(ctx context.Context, req *pb.ClientRequest, phase string) bool {
	n.mu.Lock()

	if n.State != "LEADER" || n.Failed {
		n.mu.Unlock()
		return false
	}

	// Assign sequence number
	n.SequenceNumber++
	seqNum := n.SequenceNumber
	n.mu.Unlock()

	if n.proposeAt(seqNum, req, phase) {
		return true
	}

	// No majority: roll the slot back so it does not leave a permanent gap that
	// blocks execution of every later entry. No other replica accepted it.
	n.mu.Lock()
	if entry, ok := n.AcceptedValues[seqNum]; ok && !entry.Committed {
		delete(n.AcceptedValues, seqNum)
		delete(n.AcceptedCount, seqNum)
		if n.SequenceNumber == seqNum {
			n.SequenceNumber--
		}
	}
	n.mu.Unlock()
	return false
}

// proposeAt runs the accept phase for one slot and commits it on a majority.
func (n *Node) proposeAt(seqNum int32, req *pb.ClientRequest, phase string) bool {
	n.mu.Lock()
	if n.State != "LEADER" || n.Failed {
		n.mu.Unlock()
		return false
	}
	ballotNum := n.BallotNumber
	epoch := n.epoch

	// Mark as PROPOSED
	n.AcceptedValues[seqNum] = &pb.LogEntry{
		SequenceNumber: seqNum,
		BallotNumber:   ballotNum,
		Request:        req,
		Phase:          phase,
		Committed:      false,
	}

	// Initialize accepted count
	n.AcceptedCount[seqNum] = make(map[int32]bool)
	n.AcceptedCount[seqNum][n.ID] = true // Count self

	n.mu.Unlock()

	log.Printf("[Node %d] Leader proposing seq=%d, phase=%s", n.ID, seqNum, phase)

	// Create accept message
	acceptMsg := &pb.AcceptMessage{
		BallotNumber:   ballotNum,
		SequenceNumber: seqNum,
		Phase:          phase,
		Request:        req,
		SenderId:       n.ID,
	}

	acceptCtx, cancel := context.WithTimeout(withEpoch(context.Background(), epoch), AcceptWait)
	defer cancel()

	// Broadcast accept to all peers
	var wg sync.WaitGroup
	for peerID, peer := range n.Peers {
		wg.Add(1)
		go func(pid int32, p pb.NodeServiceClient) {
			defer wg.Done()
			resp, err := p.SendAccept(acceptCtx, acceptMsg)
			if err == nil && resp.Accepted {
				n.handleAcceptedReply(seqNum, pid, ballotNum)
			}
		}(peerID, peer)
	}

	// Wait briefly for responses (don't block forever)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-acceptCtx.Done():
	}

	// Check if we have quorum
	n.mu.Lock()
	acceptCount := len(n.AcceptedCount[seqNum])
	hasQuorum := acceptCount >= n.majority() && n.State == "LEADER" && n.BallotNumber == ballotNum && n.epoch == epoch
	n.mu.Unlock()

	if hasQuorum {
		log.Printf("[Node %d] Quorum reached for seq=%d (%d/%d)", n.ID, seqNum, acceptCount, len(n.ClusterNodes))
		// Broadcast commit
		n.broadcastCommit(seqNum, req, phase, epoch)
		return true
	}

	log.Printf("[Node %d] No quorum for seq=%d (%d/%d)", n.ID, seqNum, acceptCount, len(n.ClusterNodes))
	return false
}

// handleAcceptedReply processes accepted messages from replicas
func (n *Node) handleAcceptedReply(seqNum, acceptorID, ballotNum int32) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.State != "LEADER" || n.BallotNumber != ballotNum {
		return
	}

	if _, ok := n.AcceptedCount[seqNum]; !ok {
		n.AcceptedCount[seqNum] = make(map[int32]bool)
	}

	n.AcceptedCount[seqNum][acceptorID] = true

	// Check if we now have quorum
	if len(n.AcceptedCount[seqNum]) >= n.majority() {
		// Quorum achieved - will commit
		log.Printf("[Node %d] Quorum achieved for seq=%d", n.ID, seqNum)
	}
}

// broadcastCommit sends commit messages to all replicas
func (n *Node) broadcastCommit(seqNum int32, req *pb.ClientRequest, phase string, epoch int64) {
	n.mu.Lock()

	// Mark as committed locally
	entry, ok := n.AcceptedValues[seqNum]
	if !ok {
		entry = &pb.LogEntry{SequenceNumber: seqNum, BallotNumber: n.BallotNumber, Request: req, Phase: phase}
		n.AcceptedValues[seqNum] = entry
	}
	entry.Committed = true
	n.CommittedLog[seqNum] = entry

	n.mu.Unlock()

	commitMsg := &pb.CommitMessage{
		SequenceNumber: seqNum,
		Request:        req,
		Phase:          phase,
	}

	// Execute locally first
	n.executeLog()

	// Broadcast to peers
	for _, peer := range n.Peers {
		go func(p pb.NodeServiceClient) {
			commitCtx, cancel := context.WithTimeout(withEpoch(context.Background(), epoch), 1*time.Second)
			defer cancel()
			p.SendCommit(commitCtx, commitMsg)
		}(peer)
	}
}

// executeLog executes committed entries in order
func (n *Node) executeLog() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Execute all consecutive committed entries starting from last executed + 1
	for {
		nextSeq := n.LastExecutedSeq + 1
		entry, ok := n.CommittedLog[nextSeq]

		if !ok || !entry.Committed {
			break // Gap in log or not committed yet
		}

		// Execute the transaction
		n.executeTransactionUnlocked(entry.Request, entry.Phase)
		n.LastExecutedSeq = nextSeq

		log.Printf("[Node %d] Executed seq=%d", n.ID, nextSeq)
	}
}

// Helper to check if data item belongs to this node's cluster
func (n *Node) isLocal(clientID int32) bool {
	switch n.ClusterID {
	case 1:
		return clientID >= 1 && clientID <= 3000
	case 2:
		return clientID >= 3001 && clientID <= 6000
	case 3:
		return clientID >= 6001 && clientID <= 9000
	}
	return false
}

// recordOutcome stores the final result of a request so a retransmission gets the
// same answer instead of running the transaction again. Every replica records it
// while executing the log, so a new leader knows it too.
// MUST be called with the lock held.
func (n *Node) recordOutcome(reqID string, success bool, message string) {
	n.clientLastReply[reqID] = &pb.ClientResponse{
		Success:   success,
		Message:   message,
		RequestId: reqID,
	}
}

// executeTransactionUnlocked executes without acquiring lock
func (n *Node) executeTransactionUnlocked(req *pb.ClientRequest, phase string) {
	if req == nil || phase == PhaseNoOp || phase == PhaseHeartbeat {
		return
	}
	if req.Type == pb.ClientRequest_TRANSFER && req.Transaction != nil {
		sender := req.Transaction.Sender
		receiver := req.Transaction.Receiver
		amount := req.Transaction.Amount
		reqID := req.RequestId

		senderLocal := n.isLocal(sender)
		receiverLocal := n.isLocal(receiver)

		// --- PHASE: COMMIT ("C") ---
		// Action: Finalize. Balance already updated in "P". Just clear WAL.
		if phase == "C" {
			delete(n.WAL, reqID)
			n.recordOutcome(reqID, true, "Cross-shard transaction committed")
			return
		}

		// --- PHASE: ABORT ("A") ---
		// Action: Undo changes using WAL.
		if phase == "A" {
			if entries, ok := n.WAL[reqID]; ok {
				for i := len(entries) - 1; i >= 0; i-- {
					n.DB.Exec("UPDATE balances SET balance = ? WHERE client_id = ?", entries[i].OldBalance, entries[i].DataItem)
				}
				delete(n.WAL, reqID)
			}
			n.recordOutcome(reqID, false, "Transaction aborted")
			return
		}

		// --- PHASE: PREPARE ("P") ---
		// Action: Apply Balance Changes & Log to WAL
		if phase == "P" {
			if _, done := n.WAL[reqID]; done {
				return // already prepared (duplicate prepare record)
			}
			// Coordinator Side (Sender)
			if senderLocal {
				n.applyUpdateWithWAL(reqID, sender, -amount)
			}
			// Participant Side (Receiver)
			if receiverLocal {
				n.applyUpdateWithWAL(reqID, receiver, amount)
			}
			return
		}

		// --- PHASE: NORMAL / INTRA-SHARD ("") ---
		// Action: Atomic Transfer (Sender & Receiver are likely local)
		if _, done := n.clientLastReply[reqID]; done {
			return // the same request was already executed at an earlier slot
		}
		if senderLocal {
			// 1. Check Balance
			var senderBalance int32
			err := n.DB.QueryRow("SELECT balance FROM balances WHERE client_id = ?", sender).Scan(&senderBalance)
			if err != nil || senderBalance < amount {
				log.Printf("[Node %d] Intra-shard execution failed: insufficient funds/error", n.ID)
				n.recordOutcome(reqID, false, "Insufficient balance")
				return
			}

			// 2. Debit Sender
			n.DB.Exec("UPDATE balances SET balance = balance - ? WHERE client_id = ?", amount, sender)

			// 3. Credit Receiver (if local)
			if receiverLocal {
				n.DB.Exec("UPDATE balances SET balance = balance + ? WHERE client_id = ?", amount, receiver)
			}
			n.recordOutcome(reqID, true, "Transaction committed")
		}
	}
}

// Helper to apply update and create WAL entry
func (n *Node) applyUpdateWithWAL(reqID string, clientID, delta int32) {
	// 1. Get Old Balance
	var oldBalance int32
	err := n.DB.QueryRow("SELECT balance FROM balances WHERE client_id = ?", clientID).Scan(&oldBalance)
	if err != nil {
		log.Printf("[Node %d] Error reading balance for WAL: %v", n.ID, err)
		return
	}

	// 2. Calculate New Balance
	newBalance := oldBalance + delta

	// 3. Update DB
	_, err = n.DB.Exec("UPDATE balances SET balance = ? WHERE client_id = ?", newBalance, clientID)
	if err != nil {
		log.Printf("[Node %d] Error executing update: %v", n.ID, err)
		return
	}

	// 4. Append to WAL
	entry := &pb.WALEntry{
		RequestId:  reqID,
		DataItem:   clientID,
		OldBalance: oldBalance,
		NewBalance: newBalance,
		Timestamp:  time.Now().Unix(),
	}
	n.WAL[reqID] = append(n.WAL[reqID], entry)
}

func (n *Node) abort2PCTransaction(ctx context.Context, req *pb.ClientRequest) {
	// Run Paxos for abort: every replica undoes the prepare using its WAL
	if !n.runPaxosConsensus(ctx, req, "A") {
		// Could not replicate the abort record; at least undo locally
		n.mu.Lock()
		walEntries := n.WAL[req.RequestId]
		for i := len(walEntries) - 1; i >= 0; i-- {
			n.DB.Exec("UPDATE balances SET balance = ? WHERE client_id = ?", walEntries[i].OldBalance, walEntries[i].DataItem)
		}
		delete(n.WAL, req.RequestId)
		n.mu.Unlock()
	}

	n.mu.Lock()
	delete(n.Locks, req.Transaction.Sender)
	n.mu.Unlock()
}

func (n *Node) recordLatency(requestID string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if startTime, ok := n.txnStartTimes[requestID]; ok {
		latency := time.Since(startTime)
		n.totalLatency += latency
		n.completedTxns++
		delete(n.txnStartTimes, requestID)
	}
}

// SendAccept handles accept messages from leader (Multi-Paxos accept phase)
func (n *Node) SendAccept(ctx context.Context, msg *pb.AcceptMessage) (*pb.AcceptedMessage, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Failed || !n.epochOK(ctx) {
		return &pb.AcceptedMessage{Accepted: false}, nil
	}

	// Reject proposals from an older ballot (and say which ballot we promised)
	if msg.BallotNumber < n.BallotNumber {
		return &pb.AcceptedMessage{Accepted: false, BallotNumber: n.BallotNumber, SenderId: n.ID}, nil
	}
	n.BallotNumber = msg.BallotNumber

	// Any message from the current leader makes us its follower
	if msg.SenderId != n.ID {
		if n.State != "FOLLOWER" || n.CurrentLeader != msg.SenderId {
			log.Printf("[Node %d] Following leader %d (ballot %d)", n.ID, msg.SenderId, msg.BallotNumber)
		}
		n.becomeFollower(msg.SenderId) // also resets the follower timer
	}

	// Handle heartbeat message. It carries the leader's last executed seq, which is
	// not 0 after the first transaction, so it is matched on the phase alone.
	if msg.Phase == PhaseHeartbeat {
		return &pb.AcceptedMessage{
			Accepted:       true,
			BallotNumber:   msg.BallotNumber,
			SequenceNumber: n.LastExecutedSeq,
			SenderId:       n.ID,
		}, nil
	}

	// A slot that is already chosen keeps its value
	if committed, ok := n.CommittedLog[msg.SequenceNumber]; ok {
		same := committed.Request != nil && msg.Request != nil &&
			committed.Request.RequestId == msg.Request.RequestId && committed.Phase == msg.Phase
		return &pb.AcceptedMessage{
			Accepted:       same,
			BallotNumber:   msg.BallotNumber,
			SequenceNumber: msg.SequenceNumber,
			SenderId:       n.ID,
		}, nil
	}

	// Store in accepted values (ACCEPTED state) for real transactions
	n.AcceptedValues[msg.SequenceNumber] = &pb.LogEntry{
		SequenceNumber: msg.SequenceNumber,
		BallotNumber:   msg.BallotNumber,
		Request:        msg.Request,
		Phase:          msg.Phase,
		Committed:      false,
	}

	// Update sequence number to max seen
	if msg.SequenceNumber > n.SequenceNumber {
		n.SequenceNumber = msg.SequenceNumber
	}

	log.Printf("[Node %d] Accepted seq=%d from leader %d", n.ID, msg.SequenceNumber, msg.SenderId)

	return &pb.AcceptedMessage{
		Accepted:       true,
		BallotNumber:   msg.BallotNumber,
		SequenceNumber: msg.SequenceNumber,
		SenderId:       n.ID,
	}, nil
}

// SendCommit handles commit messages (Multi-Paxos commit phase)
func (n *Node) SendCommit(ctx context.Context, msg *pb.CommitMessage) (*pb.Empty, error) {
	n.mu.Lock()

	if n.Failed || !n.epochOK(ctx) {
		n.mu.Unlock()
		return &pb.Empty{}, nil
	}

	seq := msg.SequenceNumber

	if _, done := n.CommittedLog[seq]; !done {
		// The commit carries the chosen value; it wins over whatever this replica
		// accepted for the slot (possibly from a deposed leader).
		ballot := int32(0)
		if accepted, ok := n.AcceptedValues[seq]; ok {
			ballot = accepted.BallotNumber
		}
		entry := &pb.LogEntry{
			SequenceNumber: seq,
			BallotNumber:   ballot,
			Request:        msg.Request,
			Phase:          msg.Phase,
			Committed:      true,
		}
		n.AcceptedValues[seq] = entry
		n.CommittedLog[seq] = entry
		if seq > n.SequenceNumber {
			n.SequenceNumber = seq
		}
		log.Printf("[Node %d] Committed seq=%d", n.ID, seq)
	}

	n.mu.Unlock()

	// Execute log in order
	n.executeLog()

	return &pb.Empty{}, nil
}

// resetFollowerTimer resets the follower timeout timer
// MUST be called with the lock held.
func (n *Node) resetFollowerTimer() {
	if n.followerTimer != nil {
		n.followerTimer.Stop()
		n.followerTimer = nil
	}
	if n.Failed {
		return
	}

	jitter := time.Duration(rand.Int63n(int64(FollowerTimeoutJitter)))
	timeoutDuration := FollowerTimeoutBase + jitter
	n.followerTimer = time.AfterFunc(timeoutDuration, func() {
		n.handleFollowerTimeout()
	})
}

// handleFollowerTimeout handles follower timeout - triggers election
func (n *Node) handleFollowerTimeout() {
	n.mu.Lock()
	if n.State == "LEADER" || n.Failed {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	log.Printf("[Node %d] Follower timeout - starting election", n.ID)
	n.runElection(false)
}

// FailNode simulates node failure
func (n *Node) FailNode(ctx context.Context, _ *pb.Empty) (*pb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Failed = true

	if n.followerTimer != nil {
		n.followerTimer.Stop()
		n.followerTimer = nil
	}
	if n.State == "LEADER" || n.IsLeader {
		// A failed leader stops heartbeating, so its followers elect a new one
		n.State = "FOLLOWER"
		n.IsLeader = false
		n.leaderGen++
	}
	log.Printf("Node %d failed", n.ID)
	return &pb.Empty{}, nil
}

// RecoverNode simulates node recovery
func (n *Node) RecoverNode(ctx context.Context, _ *pb.Empty) (*pb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	wasFailed := n.Failed
	n.Failed = false

	if !wasFailed {
		// Already up: keep whatever role it has
		if n.State != "LEADER" {
			n.resetFollowerTimer()
		}
		return &pb.Empty{}, nil
	}

	if n.State == "LEADER" || n.IsLeader {
		log.Printf("Node %d recovering: Stepping down from LEADER to FOLLOWER", n.ID)
	}
	// Rejoin as a follower; the leader's heartbeats bring the log up to date.
	n.becomeFollower(n.CurrentLeader)

	log.Printf("Node %d recovered", n.ID)
	return &pb.Empty{}, nil
}

// Flush clears all state for new test set
func (n *Node) Flush() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Messages still in flight from the previous set are dropped from now on
	n.epoch++

	// Reset all state
	n.SequenceNumber = 0
	n.LastExecutedSeq = 0
	n.AcceptedValues = make(map[int32]*pb.LogEntry)
	n.CommittedLog = make(map[int32]*pb.LogEntry)
	n.AcceptedCount = make(map[int32]map[int32]bool)
	n.Locks = make(map[int32]bool)
	n.WAL = make(map[string][]*pb.WALEntry)
	n.ViewChanges = make([]*pb.NewViewMessage, 0)
	n.txnStartTimes = make(map[string]time.Time)
	n.completedTxns = 0
	n.totalLatency = 0
	n.PeerSyncStatus = make(map[int32]bool)
	n.preparedTxns = make(map[string]int32)
	n.Recovering = false

	n.clientLastReply = make(map[string]*pb.ClientResponse)
	n.seenRequests = make(map[string]int64)
	// Reset database balances
	dataRange := n.getDataRangeForCluster()
	stmt, _ := n.DB.Prepare("UPDATE balances SET balance = 10 WHERE client_id >= ? AND client_id <= ?")
	stmt.Exec(dataRange[0], dataRange[1])
	stmt.Close()
}

func (n *Node) getDataRangeForCluster() [2]int32 {
	switch n.ClusterID {
	case 1:
		return [2]int32{1, 3000}
	case 2:
		return [2]int32{3001, 6000}
	case 3:
		return [2]int32{6001, 9000}
	}
	return [2]int32{0, 0}
}
