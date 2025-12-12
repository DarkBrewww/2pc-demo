package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "2pc-DarkBrewww/proto"

	"google.golang.org/grpc"
)

type Client struct {
	// Leader connections for each cluster
	leaders map[int32]pb.NodeServiceClient

	// All node connections for each cluster (for retries)
	clusterNodes map[int32][]pb.NodeServiceClient

	// Performance metrics
	mu               sync.Mutex
	totalLatency     time.Duration
	completedTxns    int64
	startTime        time.Time
	transactionTimes map[string]time.Time
}

func NewClient() *Client {
	return &Client{
		leaders:          make(map[int32]pb.NodeServiceClient),
		clusterNodes:     make(map[int32][]pb.NodeServiceClient),
		transactionTimes: make(map[string]time.Time),
	}
}

// ConnectToCluster establishes connections to all nodes in a cluster
func (c *Client) ConnectToCluster(clusterID int32, nodeAddresses []string) error {
	clients := make([]pb.NodeServiceClient, 0)

	for i, addr := range nodeAddresses {
		conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %v", addr, err)
		}

		client := pb.NewNodeServiceClient(conn)
		clients = append(clients, client)

		// First node is the default leader
		if i == 0 {
			c.leaders[clusterID] = client
		}
	}

	c.clusterNodes[clusterID] = clients
	return nil
}

// GetShardForClient determines which cluster holds a client's data
func (c *Client) GetShardForClient(clientID int32) int32 {
	if clientID >= 1 && clientID <= 3000 {
		return 1
	} else if clientID >= 3001 && clientID <= 6000 {
		return 2
	} else if clientID >= 6001 && clientID <= 9000 {
		return 3
	}
	return -1
}

// SendBalanceRequest sends a read-only balance query
func (c *Client) SendBalanceRequest(ctx context.Context, clientID int32) (*pb.ClientResponse, error) {
	clusterID := c.GetShardForClient(clientID)
	if clusterID == -1 {
		return nil, fmt.Errorf("invalid client ID: %d", clientID)
	}

	requestID := fmt.Sprintf("balance_%d_%d", clientID, time.Now().UnixNano())

	req := &pb.ClientRequest{
		Type:      pb.ClientRequest_BALANCE,
		ClientId:  clientID,
		Timestamp: time.Now().Unix(),
		RequestId: requestID,
	}

	c.mu.Lock()
	c.transactionTimes[requestID] = time.Now()
	c.mu.Unlock()

	leader := c.leaders[clusterID]
	resp, err := leader.ProcessRequest(ctx, req)

	if err != nil || !resp.Success {
		// Retry with other nodes
		return c.retryRequest(ctx, clusterID, req)
	}

	c.recordLatency(requestID)
	return resp, nil
}

// SendTransferRequest sends a transfer transaction
func (c *Client) SendTransferRequest(ctx context.Context, sender, receiver, amount int32) (*pb.ClientResponse, error) {
	senderCluster := c.GetShardForClient(sender)
	if senderCluster == -1 {
		return nil, fmt.Errorf("invalid sender ID: %d", sender)
	}

	requestID := fmt.Sprintf("transfer_%d_%d_%d", sender, receiver, time.Now().UnixNano())

	req := &pb.ClientRequest{
		Type: pb.ClientRequest_TRANSFER,
		Transaction: &pb.Transaction{
			Sender:   sender,
			Receiver: receiver,
			Amount:   amount,
		},
		Timestamp: time.Now().Unix(),
		RequestId: requestID,
	}

	c.mu.Lock()
	if c.startTime.IsZero() {
		c.startTime = time.Now()
	}
	c.transactionTimes[requestID] = time.Now()
	c.mu.Unlock()

	// Send to the leader of sender's cluster (coordinator)
	leader := c.leaders[senderCluster]
	resp, err := leader.ProcessRequest(ctx, req)

	if err != nil || !resp.Success {
		// Retry with other nodes
		return c.retryRequest(ctx, senderCluster, req)
	}

	c.recordLatency(requestID)
	return resp, nil
}

// retryRequest retries a request with different nodes in the cluster
func (c *Client) retryRequest(ctx context.Context, clusterID int32, req *pb.ClientRequest) (*pb.ClientResponse, error) {
	nodes := c.clusterNodes[clusterID]

	// Try the whole cluster up to 3 times
	maxClusterRetries := 3

	for attempt := 0; attempt < maxClusterRetries; attempt++ {
		// Try every node in the cluster
		for _, node := range nodes {
			resp, err := node.ProcessRequest(ctx, req)
			if err == nil && resp.Success {
				c.recordLatency(req.RequestId)
				return resp, nil
			}
		}

		// If we are here, all nodes failed.
		// Wait a bit for a new leader to be elected before trying the cluster again.
		if attempt < maxClusterRetries-1 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	return &pb.ClientResponse{
		Success: false,
		Message: "All nodes failed or unavailable after retries",
	}, fmt.Errorf("all retry attempts failed")
}

func (c *Client) recordLatency(requestID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if startTime, ok := c.transactionTimes[requestID]; ok {
		latency := time.Since(startTime)
		c.totalLatency += latency
		c.completedTxns++
		delete(c.transactionTimes, requestID)
	}
}

// GetPerformanceMetrics returns throughput and average latency
func (c *Client) GetPerformanceMetrics() (throughput float64, avgLatency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.completedTxns == 0 {
		return 0, 0
	}

	elapsed := time.Since(c.startTime).Seconds()
	throughput = float64(c.completedTxns) / elapsed
	avgLatency = c.totalLatency / time.Duration(c.completedTxns)

	return throughput, avgLatency
}

// ResetMetrics resets performance tracking
func (c *Client) ResetMetrics() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.totalLatency = 0
	c.completedTxns = 0
	c.startTime = time.Time{}
	c.transactionTimes = make(map[string]time.Time)
}

// FailNode sends a fail command to a specific node
func (c *Client) FailNode(ctx context.Context, clusterID, nodeIndex int32) error {
	if int(nodeIndex) >= len(c.clusterNodes[clusterID]) {
		return fmt.Errorf("invalid node index")
	}

	node := c.clusterNodes[clusterID][nodeIndex]
	_, err := node.FailNode(ctx, &pb.Empty{})
	if err != nil {
		return err
	}

	log.Printf("Node in cluster %d at index %d failed", clusterID, nodeIndex)
	return nil
}

// RecoverNode sends a recover command to a specific node
func (c *Client) RecoverNode(ctx context.Context, clusterID, nodeIndex int32) error {
	if int(nodeIndex) >= len(c.clusterNodes[clusterID]) {
		return fmt.Errorf("invalid node index")
	}

	node := c.clusterNodes[clusterID][nodeIndex]
	_, err := node.RecoverNode(ctx, &pb.Empty{})
	if err != nil {
		return err
	}

	log.Printf("Node in cluster %d at index %d recovered", clusterID, nodeIndex)
	return nil
}

// PrintBalance retrieves and prints balance from all nodes in a cluster
func (c *Client) PrintBalance(ctx context.Context, clientID int32) {
	clusterID := c.GetShardForClient(clientID)
	if clusterID == -1 {
		fmt.Printf("Invalid client ID: %d\n", clientID)
		return
	}

	nodes := c.clusterNodes[clusterID]
	balances := make([]string, 0)

	for i, node := range nodes {
		req := &pb.BalanceRequest{ClientId: clientID}
		resp, err := node.GetBalance(ctx, req)

		if err != nil {
			balances = append(balances, fmt.Sprintf("n_%d: ERROR", i+1+int(clusterID-1)*3))
		} else {
			balances = append(balances, fmt.Sprintf("n_%d: %d", resp.NodeId, resp.Balance))
		}
	}

	// Print output
	output := ""
	for i, bal := range balances {
		if i > 0 {
			output += ", "
		}
		output += bal
	}
	fmt.Printf("Input: PrintBalance(%d); Output: %s\n", clientID, output)
}

// PrintDB prints modified balances from all nodes in parallel
func (c *Client) PrintDB(ctx context.Context, modifiedClients []int32) {
	fmt.Println("PrintDB Output:")

	// 1. Group clients by cluster (Shard) to know what to query where
	clientsByCluster := make(map[int32][]int32)
	for _, clientID := range modifiedClients {
		clusterID := c.GetShardForClient(clientID)
		clientsByCluster[clusterID] = append(clientsByCluster[clusterID], clientID)
	}

	// 2. Prepare for parallel execution
	var wg sync.WaitGroup
	results := make([]string, 9) // Buffer to store results for nodes 1-9

	// 3. Launch 9 goroutines (one per node)
	for i := 0; i < 9; i++ {
		wg.Add(1)
		nodeID := int32(i + 1)

		go func(nID int32, idx int) {
			defer wg.Done()

			// Calculate cluster and index
			// Nodes 1-3 -> Cluster 1; Nodes 4-6 -> Cluster 2; Nodes 7-9 -> Cluster 3
			clusterID := (nID-1)/3 + 1
			nodeIdx := (nID - 1) % 3

			// Skip if no relevant data for this cluster
			clients := clientsByCluster[clusterID]
			if len(clients) == 0 {
				return
			}

			// Get the gRPC client connection
			if len(c.clusterNodes[clusterID]) <= int(nodeIdx) {
				results[idx] = fmt.Sprintf("Node n_%d: Connection Error", nID)
				return
			}
			nodeClient := c.clusterNodes[clusterID][nodeIdx]

			// Build the output string
			output := fmt.Sprintf("Node n_%d: ", nID)

			for j, clientID := range clients {
				req := &pb.BalanceRequest{ClientId: clientID}
				resp, err := nodeClient.GetBalance(ctx, req)

				if err != nil {
					output += fmt.Sprintf("Client %d: ERROR", clientID)
				} else {
					output += fmt.Sprintf("Client %d: %d", clientID, resp.Balance)
				}

				if j < len(clients)-1 {
					output += ", "
				}
			}
			results[idx] = output
		}(nodeID, i)
	}

	// 4. Wait for all RPCs to complete and print in order
	wg.Wait()
	for _, res := range results {
		if res != "" {
			fmt.Println(res)
		}
	}
}

// Performance prints throughput and latency
func (c *Client) Performance() {
	throughput, avgLatency := c.GetPerformanceMetrics()
	fmt.Printf("Performance Metrics:\n")
	fmt.Printf("  Throughput: %.2f transactions/second\n", throughput)
	fmt.Printf("  Average Latency: %v\n", avgLatency)
	fmt.Printf("  Total Transactions: %d\n", c.completedTxns)
}
