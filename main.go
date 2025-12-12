package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"2pc-DarkBrewww/benchmark"
	"2pc-DarkBrewww/client"
	"2pc-DarkBrewww/node"
	pb "2pc-DarkBrewww/proto"
	"2pc-DarkBrewww/reshard"

	"google.golang.org/grpc"
)

type System struct {
	nodes               []*node.Node
	servers             []*grpc.Server
	client              *client.Client
	ctx                 context.Context
	cancel              context.CancelFunc
	lastModifiedClients map[int32]bool        // Track modified clients from last test set
	transactionHistory  []reshard.Transaction // Track all transactions for resharding
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: distributed-txn.exe <test_file.csv>")
		fmt.Println("Example: distributed-txn.exe tests\\test_input.csv")
		return
	}

	testFile := os.Args[1]

	// Initialize system
	sys, err := InitializeSystem()
	if err != nil {
		log.Fatalf("Failed to initialize system: %v", err)
	}
	defer sys.Shutdown()

	// Run tests
	err = sys.RunTests(testFile)
	if err != nil {
		log.Fatalf("Failed to run tests: %v", err)
	}
}

func InitializeSystem() (*System, error) {
	ctx, cancel := context.WithCancel(context.Background())

	sys := &System{
		nodes:               make([]*node.Node, 9),
		servers:             make([]*grpc.Server, 9),
		client:              client.NewClient(),
		ctx:                 ctx,
		cancel:              cancel,
		lastModifiedClients: make(map[int32]bool),
		transactionHistory:  make([]reshard.Transaction, 0),
	}

	// Define node configurations
	// Cluster 1: nodes 1-3 (ports 50051-50053)
	// Cluster 2: nodes 4-6 (ports 50054-50056)
	// Cluster 3: nodes 7-9 (ports 50057-50059)

	nodeConfigs := []struct {
		id        int32
		clusterID int32
		port      string
		dataRange [2]int32
	}{
		// Cluster 1
		{1, 1, "localhost:50051", [2]int32{1, 3000}},
		{2, 1, "localhost:50052", [2]int32{1, 3000}},
		{3, 1, "localhost:50053", [2]int32{1, 3000}},
		// Cluster 2
		{4, 2, "localhost:50054", [2]int32{3001, 6000}},
		{5, 2, "localhost:50055", [2]int32{3001, 6000}},
		{6, 2, "localhost:50056", [2]int32{3001, 6000}},
		// Cluster 3
		{7, 3, "localhost:50057", [2]int32{6001, 9000}},
		{8, 3, "localhost:50058", [2]int32{6001, 9000}},
		{9, 3, "localhost:50059", [2]int32{6001, 9000}},
	}

	// Cluster node addresses
	clusterAddrs := map[int32][]string{
		1: {"localhost:50051", "localhost:50052", "localhost:50053"},
		2: {"localhost:50054", "localhost:50055", "localhost:50056"},
		3: {"localhost:50057", "localhost:50058", "localhost:50059"},
	}

	// Initialize all nodes
	var wg sync.WaitGroup
	for i, config := range nodeConfigs {
		wg.Add(1)
		go func(idx int, cfg struct {
			id        int32
			clusterID int32
			port      string
			dataRange [2]int32
		}) {
			defer wg.Done()

			// Create node
			n, err := node.NewNode(cfg.id, cfg.clusterID, cfg.port, clusterAddrs[cfg.clusterID], cfg.dataRange)
			if err != nil {
				log.Fatalf("Failed to create node %d: %v", cfg.id, err)
			}

			// Set initial leaders (n1, n4, n7)
			if cfg.id == 1 || cfg.id == 4 || cfg.id == 7 {
				n.State = "LEADER"
				n.IsLeader = true
				n.CurrentLeader = cfg.id
				n.BallotNumber = 1
			} else {
				n.State = "FOLLOWER"
				n.IsLeader = false
				if cfg.clusterID == 1 {
					n.CurrentLeader = 1
				} else if cfg.clusterID == 2 {
					n.CurrentLeader = 4
				} else if cfg.clusterID == 3 {
					n.CurrentLeader = 7
				}
			}

			sys.nodes[idx] = n

			// Start gRPC server
			lis, err := net.Listen("tcp", cfg.port)
			if err != nil {
				log.Fatalf("Failed to listen on %s: %v", cfg.port, err)
			}

			grpcServer := grpc.NewServer()
			pb.RegisterNodeServiceServer(grpcServer, n)
			sys.servers[idx] = grpcServer

			log.Printf("Node %d started on %s (Cluster %d)", cfg.id, cfg.port, cfg.clusterID)

			go func() {
				if err := grpcServer.Serve(lis); err != nil {
					log.Printf("Server %d error: %v", cfg.id, err)
				}
			}()
		}(i, config)
	}

	wg.Wait()
	time.Sleep(1 * time.Second) // Wait for servers to start

	// Connect nodes to their peers
	err := sys.ConnectPeers()
	if err != nil {
		return nil, err
	}

	// Start heartbeats for initial leaders and follower timers
	for _, n := range sys.nodes {
		if n.IsLeader {
			// Start heartbeat for leaders
			log.Printf("Starting heartbeat for leader node %d", n.ID)
			n.StartHeartbeat(ctx)
		} else {
			// Start follower timer for non-leaders
			n.StartFollowerTimer()
		}
	}

	// Connect client to all clusters
	for clusterID, addrs := range clusterAddrs {
		err := sys.client.ConnectToCluster(clusterID, addrs)
		if err != nil {
			return nil, fmt.Errorf("failed to connect client to cluster %d: %v", clusterID, err)
		}
	}

	log.Println("System initialized successfully")
	return sys, nil
}

func (sys *System) ConnectPeers() error {
	// Connect each node to its cluster peers
	clusterNodes := map[int32][]*node.Node{
		1: {sys.nodes[0], sys.nodes[1], sys.nodes[2]},
		2: {sys.nodes[3], sys.nodes[4], sys.nodes[5]},
		3: {sys.nodes[6], sys.nodes[7], sys.nodes[8]},
	}

	for clusterID, nodes := range clusterNodes {
		for _, n := range nodes {
			// Connect to peers in same cluster
			for _, peer := range nodes {
				if peer.ID != n.ID {
					conn, err := grpc.Dial(peer.Port, grpc.WithInsecure())
					if err != nil {
						return err
					}
					client := pb.NewNodeServiceClient(conn)
					n.Peers[peer.ID] = client
				}
			}

			// Connect to leaders of other clusters
			for otherClusterID := int32(1); otherClusterID <= 3; otherClusterID++ {
				if otherClusterID != clusterID {
					// Connect to first node (default leader) of other cluster
					leaderNode := clusterNodes[otherClusterID][0]
					conn, err := grpc.Dial(leaderNode.Port, grpc.WithInsecure())
					if err != nil {
						return err
					}
					client := pb.NewNodeServiceClient(conn)
					n.CoordinatorClients[otherClusterID] = client
				}
			}
		}
	}

	return nil
}

func (sys *System) RunTests(testFile string) error {
	// Read CSV file
	file, err := os.Open(testFile)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Allow variable number of fields
	reader.TrimLeadingSpace = true

	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	// Skip header
	if len(records) > 0 {
		records = records[1:]
	}

	// Parse test sets
	testSets := sys.ParseTestSets(records)

	var setNumbers []int
	for setNum := range testSets {
		setNumbers = append(setNumbers, setNum)
	}
	sort.Ints(setNumbers) // Sorts 1, 2, 3, 4...

	// Process each test set IN ORDER
	for _, setNum := range setNumbers {
		testSet := testSets[setNum]

		fmt.Printf("\n=== Test Set %d ===\n", setNum)
		fmt.Printf("Press Enter to process test set %d...", setNum)
		fmt.Scanln()

		// Process transactions in this set
		sys.ProcessTestSet(testSet)

		// Allow user to run commands
		sys.InteractiveCommands(testSet)

		// Flush system for next test set
		sys.FlushSystem()
	}

	return nil
}

type TestSet struct {
	SetNumber    int
	Transactions []string
	LiveNodes    []int32
}

func (sys *System) ParseTestSets(records [][]string) map[int]*TestSet {
	testSets := make(map[int]*TestSet)

	for _, record := range records {
		if len(record) < 3 {
			continue
		}

		setNum, _ := strconv.Atoi(record[0])
		transaction := strings.TrimSpace(record[1])
		liveNodesStr := strings.Trim(record[2], "[]")

		if _, exists := testSets[setNum]; !exists {
			testSets[setNum] = &TestSet{
				SetNumber:    setNum,
				Transactions: make([]string, 0),
				LiveNodes:    make([]int32, 0),
			}
		}

		testSets[setNum].Transactions = append(testSets[setNum].Transactions, transaction)

		// Parse live nodes (only on first transaction of set)
		if len(testSets[setNum].LiveNodes) == 0 {
			nodeStrs := strings.Split(liveNodesStr, ",")
			for _, nodeStr := range nodeStrs {
				nodeStr = strings.TrimSpace(nodeStr)
				nodeStr = strings.TrimPrefix(nodeStr, "n")
				if nodeID, err := strconv.Atoi(nodeStr); err == nil {
					testSets[setNum].LiveNodes = append(testSets[setNum].LiveNodes, int32(nodeID))
				}
			}
		}
	}

	return testSets
}

func (sys *System) ProcessTestSet(testSet *TestSet) {
	ctx := context.Background()

	// 1. Identify all live nodes for this test set
	liveNodeMap := make(map[int32]bool)
	for _, nodeID := range testSet.LiveNodes {
		liveNodeMap[nodeID] = true
	}

	// 2. Enforce initial state: Fail dead nodes, Recover live ones
	for _, n := range sys.nodes {
		if !liveNodeMap[n.ID] {
			// Fail nodes not in the live list
			n.FailNode(ctx, &pb.Empty{})
		} else {
			// Recover nodes in the live list (resets their follower timer)
			n.RecoverNode(ctx, &pb.Empty{})
		}
	}

	// in each cluster to start an election immediately.
	clusterMembers := map[int32][]int32{
		1: {1, 2, 3},
		2: {4, 5, 6},
		3: {7, 8, 9},
	}

	for clusterID := int32(1); clusterID <= 3; clusterID++ {
		members := clusterMembers[clusterID]

		// Find the lowest ID node that is currently ALIVE
		var candidate *node.Node
		for _, nodeID := range members {
			if liveNodeMap[nodeID] {
				// sys.nodes is 0-indexed, so nodeID 1 is at index 0
				candidate = sys.nodes[nodeID-1]
				break
			}
		}

		// If we found a live candidate, jumpstart election immediately
		if candidate != nil {
			log.Printf("Jumpstarting election for Cluster %d on Node %d", clusterID, candidate.ID)
			go candidate.StartLeaderElection(ctx)
		}
	}

	// Wait briefly for elections to settle before sending transactions
	time.Sleep(1 * time.Second)

	// 4. Track modified clients for PrintDB
	modifiedClients := make(map[int32]bool)

	// 5. Process transactions and special commands strictly in order
	for _, txn := range testSet.Transactions {
		txn = strings.TrimSpace(txn)

		// --- Handle Fail Command F(n_x) ---
		if strings.HasPrefix(txn, "F(") {
			nodeStr := strings.TrimPrefix(txn, "F(n")
			nodeStr = strings.TrimSuffix(nodeStr, ")")
			nodeID, _ := strconv.Atoi(nodeStr)
			if nodeID >= 1 && nodeID <= 9 {
				sys.nodes[nodeID-1].FailNode(ctx, &pb.Empty{})
				// Update live map so we don't accidentally jumpstart it if logic reruns (optional)
				liveNodeMap[int32(nodeID)] = false
			}
			continue
		}

		// --- Handle Recover Command R(n_x) ---
		if strings.HasPrefix(txn, "R(") {
			nodeStr := strings.TrimPrefix(txn, "R(n")
			nodeStr = strings.TrimSuffix(nodeStr, ")")
			nodeID, _ := strconv.Atoi(nodeStr)
			if nodeID >= 1 && nodeID <= 9 {
				sys.nodes[nodeID-1].RecoverNode(ctx, &pb.Empty{})
				liveNodeMap[int32(nodeID)] = true
			}
			continue
		}

		// --- Handle Standard Transactions ---
		txn = strings.Trim(txn, "()")
		parts := strings.Split(txn, ",")

		if len(parts) == 1 {
			// Balance query (s)
			clientID, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
			resp, err := sys.client.SendBalanceRequest(ctx, int32(clientID))
			if err != nil {
				log.Printf("Balance query failed: %v", err)
			} else {
				log.Printf("Balance of %d: %d", clientID, resp.Balance)
			}

		} else if len(parts) >= 3 {
			// Transfer transaction (s, r, amt)
			sender, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
			receiver, _ := strconv.Atoi(strings.TrimSpace(parts[1]))
			amount, _ := strconv.Atoi(strings.TrimSpace(parts[2]))

			modifiedClients[int32(sender)] = true
			modifiedClients[int32(receiver)] = true

			// Track transaction for resharding analysis
			sys.transactionHistory = append(sys.transactionHistory, reshard.Transaction{
				Sender:   int32(sender),
				Receiver: int32(receiver),
				Amount:   int32(amount),
			})

			// Send request
			resp, err := sys.client.SendTransferRequest(ctx, int32(sender), int32(receiver), int32(amount))
			if err != nil {
				log.Printf("Transfer failed: %v", err)
			} else if resp.Success {
				log.Printf("Transfer successful: %d -> %d (amount: %d)", sender, receiver, amount)
			} else {
				log.Printf("Transfer rejected: %s", resp.Message)
			}
		}
	}

	// 6. Store modified clients for PrintDB to use later
	sys.lastModifiedClients = modifiedClients

	// Wait for transactions to settle
	time.Sleep(500 * time.Millisecond)
}

func (sys *System) InteractiveCommands(testSet *TestSet) {
	ctx := context.Background()

	for {
		fmt.Println("\nAvailable commands:")
		fmt.Println("  1. PrintBalance(<client_id>)")
		fmt.Println("  2. PrintDB")
		fmt.Println("  3. PrintView")
		fmt.Println("  4. Performance")
		fmt.Println("  5. RunBenchmark")
		fmt.Println("  6. PrintReshard")
		fmt.Println("  7. Continue to next set")
		fmt.Print("Enter command: ")

		var cmd string
		fmt.Scanln(&cmd)

		if cmd == "7" || cmd == "" {
			break
		}

		switch {
		case strings.HasPrefix(cmd, "PrintBalance"):
			// Parse client ID
			start := strings.Index(cmd, "(")
			end := strings.Index(cmd, ")")
			if start != -1 && end != -1 {
				clientIDStr := cmd[start+1 : end]
				clientID, _ := strconv.Atoi(clientIDStr)
				sys.client.PrintBalance(ctx, int32(clientID))
			}
		case cmd == "PrintDB" || cmd == "2":
			// Get modified clients from last test set
			modifiedClients := make([]int32, 0)
			for clientID := range sys.lastModifiedClients {
				modifiedClients = append(modifiedClients, clientID)
			}
			if len(modifiedClients) == 0 {
				fmt.Println("No modified clients in this test set")
			} else {
				sys.client.PrintDB(ctx, modifiedClients)
			}
		case cmd == "PrintView" || cmd == "3":
			sys.PrintView()
		case cmd == "Performance" || cmd == "4":
			sys.client.Performance()
		case cmd == "RunBenchmark" || cmd == "5":
			sys.RunBenchmarkMenu(ctx)
		case cmd == "PrintReshard" || cmd == "6":
			sys.PrintReshard(ctx)
		}
	}
}

func (sys *System) PrintView() {
	fmt.Println("\nView Changes:")
	for _, n := range sys.nodes {
		if len(n.ViewChanges) > 0 {
			fmt.Printf("Node %d view changes:\n", n.ID)
			for _, vc := range n.ViewChanges {
				fmt.Printf("  Ballot: %d, New Leader: %d, Timestamp: %d\n",
					vc.BallotNumber, vc.NewLeaderId, vc.Timestamp)
			}
		}
	}
}

// RunBenchmarkMenu displays benchmark options and executes selected benchmark
func (sys *System) RunBenchmarkMenu(ctx context.Context) {
	fmt.Println("\n=== Benchmark Menu ===")
	fmt.Println("1. SmallBank (1,000 transactions)")
	fmt.Println("2. SmallBank (5,000 transactions)")
	fmt.Println("3. SmallBank (10,000 transactions)")
	fmt.Println("4. YCSB Workload A (Update heavy - 50% read, 50% write)")
	fmt.Println("5. YCSB Workload B (Read mostly - 95% read, 5% write)")
	fmt.Println("6. YCSB Workload C (Read only - 100% read)")
	fmt.Println("7. YCSB Workload D (Read latest)")
	fmt.Println("8. YCSB Workload F (Read-modify-write)")
	fmt.Println("9. Custom benchmark configuration")
	fmt.Print("Select benchmark (1-9): ")

	var choice string
	fmt.Scanln(&choice)

	switch choice {
	case "1":
		fmt.Println("\nRunning SmallBank with 1,000 transactions...")
		benchmark.RunSmallBank(ctx, sys.client, 9000, 1000)
	case "2":
		fmt.Println("\nRunning SmallBank with 5,000 transactions...")
		benchmark.RunSmallBank(ctx, sys.client, 9000, 5000)
	case "3":
		fmt.Println("\nRunning SmallBank with 10,000 transactions...")
		benchmark.RunSmallBank(ctx, sys.client, 9000, 10000)
	case "4":
		fmt.Println("\nRunning YCSB Workload A (Update heavy)...")
		benchmark.RunYCSB(ctx, sys.client, "A", 1000)
	case "5":
		fmt.Println("\nRunning YCSB Workload B (Read mostly)...")
		benchmark.RunYCSB(ctx, sys.client, "B", 1000)
	case "6":
		fmt.Println("\nRunning YCSB Workload C (Read only)...")
		benchmark.RunYCSB(ctx, sys.client, "C", 1000)
	case "7":
		fmt.Println("\nRunning YCSB Workload D (Read latest)...")
		benchmark.RunYCSB(ctx, sys.client, "D", 1000)
	case "8":
		fmt.Println("\nRunning YCSB Workload F (Read-modify-write)...")
		benchmark.RunYCSB(ctx, sys.client, "F", 1000)
	case "9":
		sys.RunCustomBenchmark(ctx)
	default:
		fmt.Println("Invalid choice")
	}
}

// RunCustomBenchmark allows user to configure benchmark parameters
func (sys *System) RunCustomBenchmark(ctx context.Context) {
	var readRatio, crossShardRatio, skewness float64
	var txns int

	fmt.Println("\n=== Custom Benchmark Configuration ===")

	fmt.Print("Read-only ratio (0.0-1.0, e.g., 0.6 for 60% reads): ")
	fmt.Scanln(&readRatio)
	if readRatio < 0 || readRatio > 1 {
		fmt.Println("Invalid ratio, using default 0.6")
		readRatio = 0.6
	}

	fmt.Print("Cross-shard transaction ratio (0.0-1.0, e.g., 0.3 for 30% cross-shard): ")
	fmt.Scanln(&crossShardRatio)
	if crossShardRatio < 0 || crossShardRatio > 1 {
		fmt.Println("Invalid ratio, using default 0.3")
		crossShardRatio = 0.3
	}

	fmt.Print("Data skewness (0.0=uniform, 1.0=highly skewed, e.g., 0.5): ")
	fmt.Scanln(&skewness)
	if skewness < 0 || skewness > 1 {
		fmt.Println("Invalid skewness, using default 0.5")
		skewness = 0.5
	}

	fmt.Print("Number of transactions: ")
	fmt.Scanln(&txns)
	if txns <= 0 {
		fmt.Println("Invalid number, using default 1000")
		txns = 1000
	}

	fmt.Printf("\nRunning custom benchmark:\n")
	fmt.Printf("  Read ratio: %.2f\n", readRatio)
	fmt.Printf("  Cross-shard ratio: %.2f\n", crossShardRatio)
	fmt.Printf("  Skewness: %.2f\n", skewness)
	fmt.Printf("  Transactions: %d\n\n", txns)

	config := benchmark.BenchmarkConfig{
		ReadOnlyRatio:     readRatio,
		CrossShardRatio:   crossShardRatio,
		Skewness:          skewness,
		TotalTransactions: txns,
		Seed:              time.Now().UnixNano(),
	}

	bench := benchmark.NewBenchmark(config, sys.client)
	results, err := bench.Run(ctx)

	if err != nil {
		fmt.Printf("Benchmark failed: %v\n", err)
		return
	}

	results.Print()
}

// PrintReshard analyzes transaction patterns and computes optimal resharding
func (sys *System) PrintReshard(ctx context.Context) {
	fmt.Println("\n=== Resharding Analysis ===")

	if len(sys.transactionHistory) == 0 {
		fmt.Println("No transaction history available for resharding analysis.")
		fmt.Println("Run some transactions first, then try again.")
		return
	}

	fmt.Printf("Analyzing %d transactions...\n", len(sys.transactionHistory))

	// Analyze current transaction pattern
	reshard.AnalyzeTransactionPattern(sys.transactionHistory)

	// Get current mapping
	currentMapping := reshard.GetInitialMapping()

	// Compute optimal resharding
	resharding := reshard.NewResharding(sys.transactionHistory, 3, currentMapping)
	result := resharding.ComputeOptimalSharding()

	// Print results
	reshard.PrintReshardResult(result)

	// Optionally simulate data movement
	if len(result.Moves) > 0 {
		fmt.Print("\nSimulate data movement? (y/n): ")
		var confirm string
		fmt.Scanln(&confirm)
		if confirm == "y" || confirm == "Y" {
			reshard.SimulateDataMovement(result.Moves)
		}
	} else {
		fmt.Println("\nNo resharding needed - current partitioning is optimal!")
	}
}

func (sys *System) FlushSystem() {
	log.Println("Flushing system state...")

	for _, n := range sys.nodes {
		n.Flush()
	}

	sys.client.ResetMetrics()

	log.Println("System state flushed")
}

func (sys *System) Shutdown() {
	log.Println("Shutting down system...")

	sys.cancel()

	for _, server := range sys.servers {
		server.GracefulStop()
	}

	for _, n := range sys.nodes {
		if n.DB != nil {
			n.DB.Close()
		}
	}

	log.Println("System shutdown complete")
}
