package node

import (
	"context"
	"log"
	"time"

	pb "2pc-DarkBrewww/proto"
)

// Send2PCPrepare handles prepare phase from coordinator
func (n *Node) Send2PCPrepare(ctx context.Context, msg *pb.TwoPCPrepare) (*pb.TwoPCPrepared, error) {
	n.mu.Lock()

	if n.Failed {
		n.mu.Unlock()
		return &pb.TwoPCPrepared{Success: false}, nil
	}

	receiver := msg.Transaction.Receiver
	//amount := msg.Transaction.Amount

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

	// Run Paxos for prepare phase
	success := n.runPaxosConsensus(ctx, msg.OriginalRequest, "P")

	if !success {
		n.mu.Lock()
		delete(n.Locks, receiver)
		n.mu.Unlock()
		return &pb.TwoPCPrepared{Success: false}, nil
	}

	// Execute transaction locally and save to WAL
	//n.executeAndLogTransaction(msg.OriginalRequest, receiver, amount)

	return &pb.TwoPCPrepared{
		Transaction: msg.Transaction,
		RequestId:   msg.RequestId,
		Success:     true,
	}, nil
}

// Send2PCCommit handles commit phase from coordinator
func (n *Node) Send2PCCommit(ctx context.Context, msg *pb.TwoPCCommit) (*pb.TwoPCAck, error) {
	n.mu.Lock()

	if n.Failed {
		n.mu.Unlock()
		return &pb.TwoPCAck{Success: false}, nil
	}

	receiver := msg.Transaction.Receiver
	n.mu.Unlock()

	// Create a request for consensus
	req := &pb.ClientRequest{
		Type:        pb.ClientRequest_TRANSFER,
		RequestId:   msg.RequestId,
		Transaction: msg.Transaction,
	}

	// Run Paxos for commit phase
	n.runPaxosConsensus(ctx, req, "C")

	// Release lock and clear WAL
	n.mu.Lock()
	delete(n.Locks, receiver)
	delete(n.WAL, msg.RequestId)
	n.mu.Unlock()

	return &pb.TwoPCAck{
		Transaction: msg.Transaction,
		RequestId:   msg.RequestId,
		Success:     true,
	}, nil
}

// Send2PCAbort handles abort phase from coordinator
func (n *Node) Send2PCAbort(ctx context.Context, msg *pb.TwoPCAbort) (*pb.TwoPCAck, error) {
	n.mu.Lock()

	if n.Failed {
		n.mu.Unlock()
		return &pb.TwoPCAck{Success: false}, nil
	}

	receiver := msg.Transaction.Receiver
	n.mu.Unlock()

	// Create a request for consensus
	req := &pb.ClientRequest{
		Type:        pb.ClientRequest_TRANSFER,
		RequestId:   msg.RequestId,
		Transaction: msg.Transaction,
	}

	// Run Paxos for abort phase
	n.runPaxosConsensus(ctx, req, "A")

	// Undo using WAL
	n.mu.Lock()
	walEntries := n.WAL[msg.RequestId]
	for _, entry := range walEntries {
		n.DB.Exec("UPDATE balances SET balance = ? WHERE client_id = ?", entry.OldBalance, entry.DataItem)
	}
	delete(n.WAL, msg.RequestId)
	delete(n.Locks, receiver)
	n.mu.Unlock()

	return &pb.TwoPCAck{
		Transaction: msg.Transaction,
		RequestId:   msg.RequestId,
		Success:     true,
	}, nil
}

// Leader election related functions
func (n *Node) SendPrepare(ctx context.Context, msg *pb.PrepareMessage) (*pb.PromiseMessage, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Failed {
		return nil, nil
	}

	// Promise if ballot is higher
	if msg.BallotNumber > n.BallotNumber {
		n.BallotNumber = msg.BallotNumber

		// Collect accepted values
		acceptedValues := make([]*pb.LogEntry, 0)
		for _, entry := range n.AcceptedValues {
			acceptedValues = append(acceptedValues, entry)
		}

		return &pb.PromiseMessage{
			BallotNumber:   msg.BallotNumber,
			SenderId:       n.ID,
			AcceptedValues: acceptedValues,
		}, nil
	}

	return nil, nil
}

func (n *Node) SendNewView(ctx context.Context, msg *pb.NewViewMessage) (*pb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Failed {
		return &pb.Empty{}, nil
	}

	n.BallotNumber = msg.BallotNumber
	n.CurrentLeader = msg.NewLeaderId
	n.IsLeader = (n.ID == msg.NewLeaderId)

	if n.IsLeader {
		n.State = "LEADER"
	} else {
		n.State = "FOLLOWER"
	}
	// Record view change
	n.ViewChanges = append(n.ViewChanges, msg)

	log.Printf("Node %d: New leader is %d with ballot %d", n.ID, msg.NewLeaderId, msg.BallotNumber)

	return &pb.Empty{}, nil
}

// StartLeaderElection initiates leader election
func (n *Node) StartLeaderElection(ctx context.Context) {
	n.mu.Lock()
	n.BallotNumber++
	ballotNum := n.BallotNumber
	n.mu.Unlock()

	log.Printf("Node %d starting leader election with ballot %d", n.ID, ballotNum)

	prepareMsg := &pb.PrepareMessage{
		BallotNumber: ballotNum,
		SenderId:     n.ID,
	}

	promiseCount := 1 // Count self
	var maxAcceptedValues []*pb.LogEntry

	// Send prepare to all peers
	for _, peer := range n.Peers {
		// Use a timeout context for prepares to avoid hanging
		pCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := peer.SendPrepare(pCtx, prepareMsg)
		cancel()

		if err == nil && resp != nil {
			promiseCount++
			if len(resp.AcceptedValues) > len(maxAcceptedValues) {
				maxAcceptedValues = resp.AcceptedValues
			}
		}
	}

	// Check for majority
	majority := (len(n.ClusterNodes) + 1) / 2
	if promiseCount >= majority {
		// Became leader
		n.mu.Lock()
		n.IsLeader = true
		n.CurrentLeader = n.ID
		n.State = "LEADER" // <--- FIX: EXPLICITLY SET STATE TO LEADER

		// Reset/Start Heartbeat immediately (optional but good practice)
		// n.StartHeartbeat(context.Background()) - handled by main.go logic mostly
		n.mu.Unlock()

		// Send new-view to all nodes
		newViewMsg := &pb.NewViewMessage{
			BallotNumber: ballotNum,
			NewLeaderId:  n.ID,
			Timestamp:    time.Now().Unix(),
		}

		// Record own view change
		n.mu.Lock()
		n.ViewChanges = append(n.ViewChanges, newViewMsg)
		n.mu.Unlock()

		for _, peer := range n.Peers {
			go func(p pb.NodeServiceClient) {
				// Fire and forget new view messages
				p.SendNewView(ctx, newViewMsg)
			}(peer)
		}

		log.Printf("Node %d became leader with ballot %d", n.ID, ballotNum)

		// Replay accepted values if any
		if len(maxAcceptedValues) > 0 {
			n.replayAcceptedValues(ctx, maxAcceptedValues)
		}
	}
}

func (n *Node) replayAcceptedValues(ctx context.Context, values []*pb.LogEntry) {
	// Sort by sequence number and replay uncommitted entries
	for _, entry := range values {
		if !entry.Committed {
			n.runPaxosConsensus(ctx, entry.Request, entry.Phase)
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
