package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"2pc-DarkBrewww/client"
)

// BenchmarkConfig holds benchmark parameters
type BenchmarkConfig struct {
	// Percentage of read-only transactions (0.0 to 1.0)
	ReadOnlyRatio float64

	// Percentage of cross-shard transactions among read-write txns (0.0 to 1.0)
	CrossShardRatio float64

	// Data distribution skewness (0.0 = uniform, 1.0 = highly skewed)
	Skewness float64

	// Total number of transactions to generate
	TotalTransactions int

	// Random seed for reproducibility
	Seed int64
}

// Benchmark represents the benchmark runner
type Benchmark struct {
	config BenchmarkConfig
	client *client.Client
	rand   *rand.Rand

	// Zipfian distribution for skewed access
	zipf *rand.Zipf
}

// NewBenchmark creates a new benchmark instance
func NewBenchmark(config BenchmarkConfig, c *client.Client) *Benchmark {
	r := rand.New(rand.NewSource(config.Seed))

	var zipf *rand.Zipf
	if config.Skewness > 0 {
		// Create Zipfian distribution
		// s parameter controls skewness: higher = more skewed
		s := 1.0 + config.Skewness*2.0 // Map 0-1 to 1-3
		v := 2.0
		zipf = rand.NewZipf(r, s, v, 9000-1)
	}

	return &Benchmark{
		config: config,
		client: c,
		rand:   r,
		zipf:   zipf,
	}
}

// Run executes the benchmark
func (b *Benchmark) Run(ctx context.Context) (*BenchmarkResults, error) {
	results := &BenchmarkResults{
		StartTime:         time.Now(),
		TotalTransactions: b.config.TotalTransactions,
	}

	for i := 0; i < b.config.TotalTransactions; i++ {
		if ctx.Err() != nil {
			break
		}

		// Decide transaction type
		isReadOnly := b.rand.Float64() < b.config.ReadOnlyRatio

		if isReadOnly {
			// Generate read-only transaction
			clientID := b.selectClient()
			resp, err := b.client.SendBalanceRequest(ctx, clientID)
			if err == nil && resp.Success {
				results.SuccessfulReads++
			} else {
				results.FailedReads++
			}
		} else {
			// Generate read-write transaction
			isCrossShard := b.rand.Float64() < b.config.CrossShardRatio

			sender, receiver := b.selectTransferPair(isCrossShard)
			amount := int32(b.rand.Intn(5) + 1) // Random amount 1-5

			// A rejected transfer (e.g. insufficient balance) is now returned without an
			// error, so count it as failed explicitly.
			resp, err := b.client.SendTransferRequest(ctx, sender, receiver, amount)
			if err == nil && resp.Success {
				results.SuccessfulWrites++
				if isCrossShard {
					results.CrossShardTxns++
				} else {
					results.IntraShardTxns++
				}
			} else {
				results.FailedWrites++
			}
		}
	}

	results.EndTime = time.Now()
	results.Duration = results.EndTime.Sub(results.StartTime)

	return results, nil
}

// selectClient selects a client ID based on distribution
func (b *Benchmark) selectClient() int32 {
	if b.config.Skewness > 0 && b.zipf != nil {
		// Use Zipfian distribution for skewed access
		return int32(b.zipf.Uint64()) + 1
	}

	// Uniform distribution
	return int32(b.rand.Intn(9000)) + 1
}

// selectTransferPair selects sender and receiver for a transfer
func (b *Benchmark) selectTransferPair(crossShard bool) (sender, receiver int32) {
	sender = b.selectClient()

	if crossShard {
		// Ensure receiver is in different shard
		senderShard := getShardForClient(sender)

		for {
			receiver = b.selectClient()
			if getShardForClient(receiver) != senderShard {
				break
			}
		}
	} else {
		// Ensure receiver is in same shard
		senderShard := getShardForClient(sender)
		shardStart, shardEnd := getShardRange(senderShard)

		for {
			receiver = b.selectClient()
			if receiver >= shardStart && receiver <= shardEnd && receiver != sender {
				break
			}
		}
	}

	return sender, receiver
}

// BenchmarkResults holds the results of a benchmark run
type BenchmarkResults struct {
	StartTime         time.Time
	EndTime           time.Time
	Duration          time.Duration
	TotalTransactions int
	SuccessfulReads   int
	FailedReads       int
	SuccessfulWrites  int
	FailedWrites      int
	IntraShardTxns    int
	CrossShardTxns    int
}

// Print displays benchmark results
func (r *BenchmarkResults) Print() {
	fmt.Println("\n=== Benchmark Results ===")
	fmt.Printf("Duration: %v\n", r.Duration)
	fmt.Printf("Total Transactions: %d\n", r.TotalTransactions)
	fmt.Printf("Successful Reads: %d\n", r.SuccessfulReads)
	fmt.Printf("Failed Reads: %d\n", r.FailedReads)
	fmt.Printf("Successful Writes: %d\n", r.SuccessfulWrites)
	fmt.Printf("Failed Writes: %d\n", r.FailedWrites)
	fmt.Printf("Intra-shard Transactions: %d\n", r.IntraShardTxns)
	fmt.Printf("Cross-shard Transactions: %d\n", r.CrossShardTxns)

	if r.Duration.Seconds() > 0 {
		throughput := float64(r.SuccessfulReads+r.SuccessfulWrites) / r.Duration.Seconds()
		fmt.Printf("Throughput: %.2f txn/sec\n", throughput)
	}

	successRate := float64(r.SuccessfulReads+r.SuccessfulWrites) / float64(r.TotalTransactions) * 100
	fmt.Printf("Success Rate: %.2f%%\n", successRate)
}

// Helper functions
func getShardForClient(clientID int32) int32 {
	if clientID >= 1 && clientID <= 3000 {
		return 1
	} else if clientID >= 3001 && clientID <= 6000 {
		return 2
	} else if clientID >= 6001 && clientID <= 9000 {
		return 3
	}
	return -1
}

func getShardRange(shardID int32) (start, end int32) {
	switch shardID {
	case 1:
		return 1, 3000
	case 2:
		return 3001, 6000
	case 3:
		return 6001, 9000
	}
	return 0, 0
}

// RunSmallBank runs the SmallBank benchmark
func RunSmallBank(ctx context.Context, c *client.Client, numAccounts int, numTransactions int) {
	fmt.Println("\n=== Running SmallBank Benchmark ===")

	config := BenchmarkConfig{
		ReadOnlyRatio:     0.6, // 60% reads (balance checks)
		CrossShardRatio:   0.3, // 30% cross-shard among writes
		Skewness:          0.5, // Moderate skew
		TotalTransactions: numTransactions,
		Seed:              time.Now().UnixNano(),
	}

	bench := NewBenchmark(config, c)
	results, err := bench.Run(ctx)

	if err != nil {
		fmt.Printf("Benchmark failed: %v\n", err)
		return
	}

	results.Print()
}

// RunYCSB runs YCSB-style workloads
func RunYCSB(ctx context.Context, c *client.Client, workload string, numTransactions int) {
	fmt.Printf("\n=== Running YCSB Workload %s ===\n", workload)

	var config BenchmarkConfig

	switch workload {
	case "A": // Update heavy
		config = BenchmarkConfig{
			ReadOnlyRatio:     0.5,
			CrossShardRatio:   0.2,
			Skewness:          0.8, // Zipfian
			TotalTransactions: numTransactions,
			Seed:              time.Now().UnixNano(),
		}
	case "B": // Read mostly
		config = BenchmarkConfig{
			ReadOnlyRatio:     0.95,
			CrossShardRatio:   0.2,
			Skewness:          0.8,
			TotalTransactions: numTransactions,
			Seed:              time.Now().UnixNano(),
		}
	case "C": // Read only
		config = BenchmarkConfig{
			ReadOnlyRatio:     1.0,
			CrossShardRatio:   0.0,
			Skewness:          0.8,
			TotalTransactions: numTransactions,
			Seed:              time.Now().UnixNano(),
		}
	case "D": // Read latest
		config = BenchmarkConfig{
			ReadOnlyRatio:     0.95,
			CrossShardRatio:   0.1,
			Skewness:          0.9, // Latest insertions
			TotalTransactions: numTransactions,
			Seed:              time.Now().UnixNano(),
		}
	case "F": // Read-modify-write
		config = BenchmarkConfig{
			ReadOnlyRatio:     0.5,
			CrossShardRatio:   0.3,
			Skewness:          0.8,
			TotalTransactions: numTransactions,
			Seed:              time.Now().UnixNano(),
		}
	default:
		fmt.Printf("Unknown workload: %s\n", workload)
		return
	}

	bench := NewBenchmark(config, c)
	results, err := bench.Run(ctx)

	if err != nil {
		fmt.Printf("Benchmark failed: %v\n", err)
		return
	}

	results.Print()
}
