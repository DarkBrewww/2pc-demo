package reshard

import (
	"fmt"
	"log"
)

// Transaction represents a transaction for resharding analysis
type Transaction struct {
	Sender   int32
	Receiver int32
	Amount   int32
}

// ShardMapping represents current shard assignment
type ShardMapping struct {
	DataItemToShard map[int32]int32 // data_item -> cluster_id
}

// ReshardResult represents the result of resharding
type ReshardResult struct {
	Moves []DataMove // List of data movements
	Cost  int        // Number of cross-shard edges in new partition
}

// DataMove represents moving a data item between clusters
type DataMove struct {
	DataItem      int32
	SourceCluster int32
	TargetCluster int32
}

// Resharding performs hypergraph partitioning
type Resharding struct {
	transactions   []Transaction
	numClusters    int32
	currentMapping *ShardMapping
}

// NewResharding creates a new resharding analyzer
func NewResharding(txns []Transaction, numClusters int32, currentMapping *ShardMapping) *Resharding {
	return &Resharding{
		transactions:   txns,
		numClusters:    numClusters,
		currentMapping: currentMapping,
	}
}

// ComputeOptimalSharding computes optimal shard assignment
// This uses a simplified greedy algorithm for hypergraph partitioning
func (r *Resharding) ComputeOptimalSharding() *ReshardResult {
	// Build hypergraph
	// Vertices = data items, Hyperedges = transactions

	// Count frequency of each data item
	itemFrequency := make(map[int32]int)
	for _, txn := range r.transactions {
		itemFrequency[txn.Sender]++
		itemFrequency[txn.Receiver]++
	}

	// Count co-occurrence (edges between data items)
	cooccurrence := make(map[[2]int32]int)
	for _, txn := range r.transactions {
		key := [2]int32{min(txn.Sender, txn.Receiver), max(txn.Sender, txn.Receiver)}
		cooccurrence[key]++
	}

	// Initialize new mapping (start with current)
	newMapping := make(map[int32]int32)
	for item, shard := range r.currentMapping.DataItemToShard {
		newMapping[item] = shard
	}

	// Get all unique data items
	allItems := make([]int32, 0)
	for item := range itemFrequency {
		allItems = append(allItems, item)
	}

	// Compute initial cluster sizes
	clusterSizes := make(map[int32]int)
	for _, shard := range newMapping {
		clusterSizes[shard]++
	}

	// Target size per cluster (balanced)
	targetSize := len(allItems) / int(r.numClusters)

	// Greedy optimization: move items to reduce cross-shard edges
	improved := true
	iterations := 0
	maxIterations := 10

	for improved && iterations < maxIterations {
		improved = false
		iterations++

		for _, item := range allItems {
			currentShard := newMapping[item]
			bestShard := currentShard
			bestScore := r.computeItemScore(item, currentShard, newMapping, cooccurrence)

			// Try moving to each other cluster
			for shard := int32(1); shard <= r.numClusters; shard++ {
				if shard == currentShard {
					continue
				}

				// Check if target shard would become too large
				if clusterSizes[shard] >= targetSize+targetSize/4 {
					continue
				}

				score := r.computeItemScore(item, shard, newMapping, cooccurrence)

				// Move if it improves the score and maintains balance
				if score > bestScore {
					bestScore = score
					bestShard = shard
					improved = true
				}
			}

			if bestShard != currentShard {
				// Update mapping
				clusterSizes[currentShard]--
				clusterSizes[bestShard]++
				newMapping[item] = bestShard
			}
		}
	}

	// Compute moves
	moves := make([]DataMove, 0)
	for item, newShard := range newMapping {
		oldShard := r.currentMapping.DataItemToShard[item]
		if oldShard != newShard {
			moves = append(moves, DataMove{
				DataItem:      item,
				SourceCluster: oldShard,
				TargetCluster: newShard,
			})
		}
	}

	// Compute cost (number of cross-shard transactions)
	cost := r.computeTotalCost(newMapping)

	return &ReshardResult{
		Moves: moves,
		Cost:  cost,
	}
}

// computeItemScore computes benefit of placing item in a shard
// Higher score = more transactions within same shard
func (r *Resharding) computeItemScore(item int32, shard int32, mapping map[int32]int32, cooccurrence map[[2]int32]int) int {
	score := 0

	// Count how many related items are in the same shard
	for _, txn := range r.transactions {
		var relatedItem int32
		if txn.Sender == item {
			relatedItem = txn.Receiver
		} else if txn.Receiver == item {
			relatedItem = txn.Sender
		} else {
			continue
		}

		if mapping[relatedItem] == shard {
			// Same shard - increase score
			score += 10
		} else {
			// Different shard - decrease score
			score -= 5
		}
	}

	return score
}

// computeTotalCost computes number of cross-shard transactions
func (r *Resharding) computeTotalCost(mapping map[int32]int32) int {
	crossShardCount := 0

	for _, txn := range r.transactions {
		senderShard := mapping[txn.Sender]
		receiverShard := mapping[txn.Receiver]

		if senderShard != receiverShard {
			crossShardCount++
		}
	}

	return crossShardCount
}

// PrintReshardResult prints the resharding result
func PrintReshardResult(result *ReshardResult) {
	fmt.Println("\n=== Reshard Result ===")
	fmt.Printf("Total Moves: %d\n", len(result.Moves))
	fmt.Printf("Cross-shard Transaction Cost: %d\n", result.Cost)

	if len(result.Moves) == 0 {
		fmt.Println("No resharding needed")
		return
	}

	fmt.Println("\nData Movements:")
	for _, move := range result.Moves {
		fmt.Printf("  (%d, c_%d, c_%d)\n", move.DataItem, move.SourceCluster, move.TargetCluster)
	}

	// Group by source and target cluster
	movesByRoute := make(map[string][]int32)
	for _, move := range result.Moves {
		key := fmt.Sprintf("c_%d -> c_%d", move.SourceCluster, move.TargetCluster)
		movesByRoute[key] = append(movesByRoute[key], move.DataItem)
	}

	fmt.Println("\nMoves by Route:")
	for route, items := range movesByRoute {
		fmt.Printf("  %s: %d items\n", route, len(items))
	}
}

// GetInitialMapping returns the default range-based shard mapping
func GetInitialMapping() *ShardMapping {
	mapping := &ShardMapping{
		DataItemToShard: make(map[int32]int32),
	}

	// Cluster 1: 1-3000
	for i := int32(1); i <= 3000; i++ {
		mapping.DataItemToShard[i] = 1
	}

	// Cluster 2: 3001-6000
	for i := int32(3001); i <= 6000; i++ {
		mapping.DataItemToShard[i] = 2
	}

	// Cluster 3: 6001-9000
	for i := int32(6001); i <= 9000; i++ {
		mapping.DataItemToShard[i] = 3
	}

	return mapping
}

// AnalyzeTransactionPattern analyzes transaction patterns
func AnalyzeTransactionPattern(transactions []Transaction) {
	fmt.Println("\n=== Transaction Pattern Analysis ===")

	totalTxns := len(transactions)
	intraShardCount := 0
	crossShardCount := 0

	// Track hotspots
	itemAccessCount := make(map[int32]int)

	mapping := GetInitialMapping()

	for _, txn := range transactions {
		senderShard := mapping.DataItemToShard[txn.Sender]
		receiverShard := mapping.DataItemToShard[txn.Receiver]

		if senderShard == receiverShard {
			intraShardCount++
		} else {
			crossShardCount++
		}

		itemAccessCount[txn.Sender]++
		itemAccessCount[txn.Receiver]++
	}

	fmt.Printf("Total Transactions: %d\n", totalTxns)
	fmt.Printf("Intra-shard: %d (%.2f%%)\n", intraShardCount, float64(intraShardCount)/float64(totalTxns)*100)
	fmt.Printf("Cross-shard: %d (%.2f%%)\n", crossShardCount, float64(crossShardCount)/float64(totalTxns)*100)

	// Find hotspots (top 10 most accessed items)
	type itemCount struct {
		item  int32
		count int
	}

	items := make([]itemCount, 0)
	for item, count := range itemAccessCount {
		items = append(items, itemCount{item, count})
	}

	// Simple sort for top 10
	for i := 0; i < len(items) && i < 10; i++ {
		maxIdx := i
		for j := i + 1; j < len(items); j++ {
			if items[j].count > items[maxIdx].count {
				maxIdx = j
			}
		}
		items[i], items[maxIdx] = items[maxIdx], items[i]
	}

	fmt.Println("\nTop 10 Hotspots:")
	for i := 0; i < len(items) && i < 10; i++ {
		fmt.Printf("  Item %d: %d accesses (Shard %d)\n",
			items[i].item, items[i].count, mapping.DataItemToShard[items[i].item])
	}
}

// SimulateDataMovement simulates moving data between clusters
func SimulateDataMovement(moves []DataMove) {
	log.Println("Starting data movement simulation...")

	// Group moves by source cluster
	movesBySource := make(map[int32][]DataMove)
	for _, move := range moves {
		movesBySource[move.SourceCluster] = append(movesBySource[move.SourceCluster], move)
	}

	// Simulate movement
	for sourceCluster, clusterMoves := range movesBySource {
		log.Printf("Moving %d items from cluster c_%d", len(clusterMoves), sourceCluster)

		// In a real implementation, this would:
		// 1. Lock the system or quiesce traffic
		// 2. Copy data from source to destination
		// 3. Update shard mapping
		// 4. Verify data integrity
		// 5. Remove data from source
		// 6. Resume normal operations

		for _, move := range clusterMoves {
			log.Printf("  Moving item %d to cluster c_%d", move.DataItem, move.TargetCluster)
		}
	}

	log.Println("Data movement completed")
}

func min(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}
