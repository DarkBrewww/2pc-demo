package node

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "2pc-DarkBrewww/proto"

	_ "github.com/mattn/go-sqlite3"
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

	// Performance metrics
	txnStartTimes map[string]time.Time
	completedTxns int64
	totalLatency  time.Duration

	// Client request deduplication
	clientLastReply map[string]*pb.ClientResponse
	seenRequests    map[string]int64

	PeerSyncStatus map[int32]bool // peer_id -> is_synced
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

	// Initialize balances for this shard's data range
	stmt, err := db.Prepare("INSERT OR REPLACE INTO balances (client_id, balance) VALUES (?, ?)")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	for i := dataRange[0]; i <= dataRange[1]; i++ {
		_, err = stmt.Exec(i, 10)
		if err != nil {
			return nil, err
		}
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
		leaderTimeout:      10 * time.Second, // Increased timeout to prevent immediate elections
		Peers:              make(map[int32]pb.NodeServiceClient),
		CoordinatorClients: make(map[int32]pb.NodeServiceClient),
		txnStartTimes:      make(map[string]time.Time),
		clientLastReply:    make(map[string]*pb.ClientResponse),
		seenRequests:       make(map[string]int64),
		PeerSyncStatus:     make(map[int32]bool),
	}

	return node, nil
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

// StartHeartbeat sends periodic heartbeats to followers
func (n *Node) StartHeartbeat(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n.mu.RLock()
				if !n.IsLeader || n.Failed || n.State != "LEADER" {
					n.mu.RUnlock()
					return
				}
				ballotNum := n.BallotNumber
				leaderSeq := n.LastExecutedSeq
				n.mu.RUnlock()

				heartbeatMsg := &pb.AcceptMessage{
					BallotNumber:   ballotNum,
					SequenceNumber: leaderSeq,
					Phase:          "HEARTBEAT",
					SenderId:       n.ID,
				}

				// CHANGE: Iterate with peerID to track status
				for peerID, peer := range n.Peers {

					// Check if we are already syncing this peer
					n.mu.Lock()
					isSyncing := n.PeerSyncStatus[peerID]
					n.mu.Unlock()

					// If syncing, SKIP this heartbeat cycle for this peer logic
					if isSyncing {
						continue
					}

					go func(pID int32, p pb.NodeServiceClient) {
						hbCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
						defer cancel()

						resp, err := p.SendAccept(hbCtx, heartbeatMsg)

						if err == nil && resp != nil {
							// If Follower is behind, trigger sync
							if resp.SequenceNumber < leaderSeq {
								// Mark as syncing to prevent overlap
								n.mu.Lock()
								n.PeerSyncStatus[pID] = true
								n.mu.Unlock()

								// Perform sync
								n.syncFollower(p, resp.SequenceNumber+1, leaderSeq)

								// Unmark when done
								n.mu.Lock()
								n.PeerSyncStatus[pID] = false
								n.mu.Unlock()
							}
						}
					}(peerID, peer)
				}
			}
		}
	}()
}

// syncFollower pushes missing logs to a stale follower
func (n *Node) syncFollower(peer pb.NodeServiceClient, startSeq, endSeq int32) {
	// Don't sync if gap is huge or invalid
	if startSeq > endSeq {
		return
	}

	// log.Printf("[Node %d] Syncing follower from %d to %d", n.ID, startSeq, endSeq)

	for seq := startSeq; seq <= endSeq; seq++ {
		n.mu.RLock()
		entry, exists := n.CommittedLog[seq]
		n.mu.RUnlock()

		if !exists {
			continue
		}

		// Re-construct the Accept Message (Proposal)
		acceptMsg := &pb.AcceptMessage{
			BallotNumber:   entry.BallotNumber,
			SequenceNumber: entry.SequenceNumber,
			Phase:          entry.Phase,
			Request:        entry.Request,
			SenderId:       n.ID,
		}

		// Re-construct the Commit Message
		commitMsg := &pb.CommitMessage{
			SequenceNumber: entry.SequenceNumber,
			Request:        entry.Request,
			Phase:          entry.Phase,
		}

		// In syncFollower:
		// Use a timeout context instead of Background()
		syncCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		peer.SendAccept(syncCtx, acceptMsg)
		peer.SendCommit(syncCtx, commitMsg)
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
		return nil, fmt.Errorf("node is failed")
	}

	// Check for duplicate request
	reqKey := req.RequestId
	if lastReply, ok := n.clientLastReply[reqKey]; ok {
		n.mu.Unlock()
		log.Printf("[Node %d] Deduplication cache hit for %s", n.ID, reqKey)
		return lastReply, nil
	}

	// Track start time for latency measurement
	n.txnStartTimes[req.RequestId] = time.Now()

	// If not leader, redirect
	if !n.IsLeader || n.State != "LEADER" {
		leaderId := n.CurrentLeader
		state := n.State
		n.mu.Unlock()

		log.Printf("[Node %d] Not leader (state=%s, leader=%d), redirecting", n.ID, state, leaderId)

		if leaderId > 0 {
			if peer, ok := n.Peers[leaderId]; ok {
				return peer.ProcessRequest(ctx, req)
			}
		}

		return &pb.ClientResponse{
			Success: false,
			Message: fmt.Sprintf("No leader available (state=%s)", state),
		}, nil
	}
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

	// Transfer transaction - requires consensus
	sender := req.Transaction.Sender
	receiver := req.Transaction.Receiver

	senderShard := n.getShardForClient(sender)
	receiverShard := n.getShardForClient(receiver)

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

	// Check locks
	if n.Locks[sender] || n.Locks[receiver] {
		n.mu.Unlock()
		return &pb.ClientResponse{
			Success:   false,
			Message:   "Data items are locked",
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
	n.mu.Unlock()

	if success {
		n.recordLatency(req.RequestId)
		return &pb.ClientResponse{
			Success:   true,
			Message:   "Transaction committed",
			RequestId: req.RequestId,
		}, nil
	}

	return &pb.ClientResponse{
		Success:   false,
		Message:   "Consensus failed",
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

	// Phase 1: Prepare phase for coordinator
	// Run Paxos with phase "P"
	success := n.runPaxosConsensus(ctx, req, "P")
	if !success {
		n.mu.Lock()
		delete(n.Locks, sender)
		n.mu.Unlock()
		return &pb.ClientResponse{
			Success:   false,
			Message:   "Coordinator prepare failed",
			RequestId: req.RequestId,
		}, nil
	}

	// Execute transaction locally and save to WAL
	//n.executeAndLogTransaction(req, sender, -amount)

	// Send prepare to participant with timeout
	participantShard := n.getShardForClient(receiver)
	participantLeader := n.getLeaderForShard(participantShard)

	prepareMsg := &pb.TwoPCPrepare{
		Transaction:     req.Transaction,
		OriginalRequest: req,
		RequestId:       req.RequestId,
	}

	// FIX 1: Add timeout context for prepare phase
	prepareCtx, prepareCancel := context.WithTimeout(ctx, 5*time.Second)
	defer prepareCancel()

	preparedResp, err := participantLeader.Send2PCPrepare(prepareCtx, prepareMsg)

	if err != nil || !preparedResp.Success {
		// Abort due to participant failure or timeout
		log.Printf("[Node %d] Participant prepare failed or timeout: %v", n.ID, err)
		n.abort2PCTransaction(ctx, req)
		n.recordLatency(req.RequestId)
		return &pb.ClientResponse{
			Success:   false,
			Message:   "Participant prepare failed or timeout",
			RequestId: req.RequestId,
		}, nil
	}

	// Phase 2: Commit phase
	commitMsg := &pb.TwoPCCommit{
		Transaction: req.Transaction,
		RequestId:   req.RequestId,
	}

	// Run Paxos for commit with phase "C"
	n.runPaxosConsensus(ctx, req, "C")

	// FIX 2: Send commit to participant with retry and wait for ACK
	maxRetries := 3
	ackReceived := false

	for i := 0; i < maxRetries; i++ {
		commitCtx, commitCancel := context.WithTimeout(ctx, 2*time.Second)
		ack, err := participantLeader.Send2PCCommit(commitCtx, commitMsg)
		commitCancel()

		if err == nil && ack != nil && ack.Success {
			ackReceived = true
			log.Printf("[Node %d] Received ACK from participant", n.ID)
			break
		}

		if i < maxRetries-1 {
			log.Printf("[Node %d] Retrying 2PC commit (%d/%d)", n.ID, i+1, maxRetries)
			time.Sleep(100 * time.Millisecond)
		}
	}

	if !ackReceived {
		log.Printf("[Node %d] Warning: ACK not received from participant after retries", n.ID)
		// Continue anyway - participant should have committed via Paxos
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
	// This should return the gRPC client for the leader of the specified shard
	return n.CoordinatorClients[shardID]
}

// runPaxosConsensus executes stable leader Multi-Paxos
func (n *Node) runPaxosConsensus(ctx context.Context, req *pb.ClientRequest, phase string) bool {
	n.mu.Lock()

	if n.State != "LEADER" {
		n.mu.Unlock()
		return false
	}

	// Assign sequence number
	n.SequenceNumber++
	seqNum := n.SequenceNumber
	ballotNum := n.BallotNumber

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

	// Broadcast accept to all peers
	var wg sync.WaitGroup
	for peerID, peer := range n.Peers {
		wg.Add(1)
		go func(pid int32, p pb.NodeServiceClient) {
			defer wg.Done()
			resp, err := p.SendAccept(ctx, acceptMsg)
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
	case <-time.After(500 * time.Millisecond):
	}

	// Check if we have quorum
	n.mu.Lock()
	majority := (len(n.ClusterNodes) + 1) / 2
	acceptCount := len(n.AcceptedCount[seqNum])
	hasQuorum := acceptCount >= majority
	n.mu.Unlock()

	if hasQuorum {
		log.Printf("[Node %d] Quorum reached for seq=%d (%d/%d)", n.ID, seqNum, acceptCount, len(n.ClusterNodes))
		// Broadcast commit
		n.broadcastCommit(ctx, seqNum, req, phase)
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
	majority := (len(n.ClusterNodes) + 1) / 2
	if len(n.AcceptedCount[seqNum]) >= majority {
		// Quorum achieved - will commit
		log.Printf("[Node %d] Quorum achieved for seq=%d", n.ID, seqNum)
	}
}

// broadcastCommit sends commit messages to all replicas
func (n *Node) broadcastCommit(ctx context.Context, seqNum int32, req *pb.ClientRequest, phase string) {
	n.mu.Lock()

	// Mark as committed locally
	if entry, ok := n.AcceptedValues[seqNum]; ok {
		entry.Committed = true
		n.CommittedLog[seqNum] = entry
	}

	n.mu.Unlock()

	commitMsg := &pb.CommitMessage{
		SequenceNumber: seqNum,
		Request:        req,
		Phase:          phase,
	}

	// Execute locally first
	n.executeLog()
	bgCtx := context.Background()

	// Broadcast to peers
	for _, peer := range n.Peers {
		go func(p pb.NodeServiceClient) {
			// Give it a reasonable timeout (e.g., 2 seconds)
			commitCtx, cancel := context.WithTimeout(bgCtx, 1*time.Second)
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

		// Store reply for client deduplication
		if n.State == "LEADER" && entry.Request != nil {
			// FIX: Use RequestId here too
			reqKey := entry.Request.RequestId
			n.clientLastReply[reqKey] = &pb.ClientResponse{
				Success:   true,
				Message:   "Transaction committed",
				RequestId: entry.Request.RequestId,
			}
		}
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

// executeTransactionUnlocked executes without acquiring lock
func (n *Node) executeTransactionUnlocked(req *pb.ClientRequest, phase string) {
	if req.Type == pb.ClientRequest_TRANSFER {
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
			return
		}

		// --- PHASE: ABORT ("A") ---
		// Action: Undo changes using WAL.
		if phase == "A" {
			if entries, ok := n.WAL[reqID]; ok {
				for _, entry := range entries {
					n.DB.Exec("UPDATE balances SET balance = ? WHERE client_id = ?", entry.OldBalance, entry.DataItem)
				}
				delete(n.WAL, reqID)
			}
			return
		}

		// --- PHASE: PREPARE ("P") ---
		// Action: Apply Balance Changes & Log to WAL
		if phase == "P" {
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
		if senderLocal {
			// 1. Check Balance
			var senderBalance int32
			err := n.DB.QueryRow("SELECT balance FROM balances WHERE client_id = ?", sender).Scan(&senderBalance)
			if err != nil || senderBalance < amount {
				log.Printf("[Node %d] Intra-shard execution failed: insufficient funds/error", n.ID)
				return
			}

			// 2. Debit Sender
			n.DB.Exec("UPDATE balances SET balance = balance - ? WHERE client_id = ?", amount, sender)

			// 3. Credit Receiver (if local)
			if receiverLocal {
				n.DB.Exec("UPDATE balances SET balance = balance + ? WHERE client_id = ?", amount, receiver)
			}
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
	// Run Paxos for abort
	n.runPaxosConsensus(ctx, req, "A")

	// Undo using WAL
	n.mu.Lock()
	walEntries := n.WAL[req.RequestId]
	for _, entry := range walEntries {
		n.DB.Exec("UPDATE balances SET balance = ? WHERE client_id = ?", entry.OldBalance, entry.DataItem)
	}
	delete(n.WAL, req.RequestId)
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

	if n.Failed {
		return &pb.AcceptedMessage{Accepted: false}, nil
	}

	// Accept if ballot number is >= current ballot
	if msg.BallotNumber >= n.BallotNumber {
		n.BallotNumber = msg.BallotNumber

		// Update leader if not self
		if msg.SenderId != n.ID && n.State != "LEADER" {
			n.State = "FOLLOWER"
			n.CurrentLeader = msg.SenderId
			n.IsLeader = false
			n.resetFollowerTimer() // Reset timer on ANY message from leader
		}

		// Handle heartbeat message
		if msg.SequenceNumber == 0 && msg.Phase == "HEARTBEAT" {
			return &pb.AcceptedMessage{
				Accepted:       true,
				BallotNumber:   msg.BallotNumber,
				SequenceNumber: n.LastExecutedSeq,
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

	return &pb.AcceptedMessage{Accepted: false}, nil
}

// SendCommit handles commit messages (Multi-Paxos commit phase)
func (n *Node) SendCommit(ctx context.Context, msg *pb.CommitMessage) (*pb.Empty, error) {
	n.mu.Lock()

	if n.Failed {
		n.mu.Unlock()
		return &pb.Empty{}, nil
	}

	seq := msg.SequenceNumber

	// Mark as committed (COMMITTED state)
	if entry, ok := n.AcceptedValues[seq]; ok {
		entry.Committed = true
		n.CommittedLog[seq] = entry
		log.Printf("[Node %d] Committed seq=%d", n.ID, seq)
	} else {
		// Commit for something we haven't accepted - store it anyway
		n.CommittedLog[seq] = &pb.LogEntry{
			SequenceNumber: seq,
			Request:        msg.Request,
			Phase:          msg.Phase,
			Committed:      true,
		}
		log.Printf("[Node %d] Committed seq=%d (late commit)", n.ID, seq)
	}

	n.mu.Unlock()

	// Execute log in order
	n.executeLog()

	return &pb.Empty{}, nil
}

// resetFollowerTimer resets the follower timeout timer
func (n *Node) resetFollowerTimer() {
	if n.followerTimer != nil {
		n.followerTimer.Stop()
	}

	jitter := time.Duration(rand.Intn(50)) * time.Millisecond
	timeoutDuration := 120*time.Millisecond + jitter
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
	n.StartLeaderElection(context.Background())
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
	// Note: Heartbeat will automatically stop when State != "LEADER" or Failed = true
	log.Printf("Node %d failed", n.ID)
	return &pb.Empty{}, nil
}

// RecoverNode simulates node recovery
func (n *Node) RecoverNode(ctx context.Context, _ *pb.Empty) (*pb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Failed = false

	if n.State == "LEADER" {
		log.Printf("Node %d recovering: Stepping down from LEADER to FOLLOWER", n.ID)
		n.State = "FOLLOWER"
		n.IsLeader = false
	}

	n.resetFollowerTimer()

	log.Printf("Node %d recovered", n.ID)
	return &pb.Empty{}, nil
}

// Flush clears all state for new test set
func (n *Node) Flush() {
	n.mu.Lock()
	defer n.mu.Unlock()

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
