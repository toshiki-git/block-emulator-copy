package partition

import (
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
)

// State of the Constraint Label Propagation Algorithm (CLPA)
type CLPAState struct {
	NetGraph          Graph          // Graph on which the CLPA algorithm needs to run
	PartitionMap      map[Vertex]int // Map recording partition information, which shard a node belongs to
	Edges2Shard       []int          // Number of edges adjacent to a shard, corresponding to "total weight of edges associated with label k" in the paper
	VertexsNumInShard []int          // Number of nodes within a shard
	WeightPenalty     float64        // Weight penalty, corresponding to "beta" in the paper
	MinEdges2Shard    int            // Minimum number of shard-adjacent edges, minimum "total weight of edges associated with label k"
	MaxIterations     int            // Maximum number of iterations, constraint, corresponding to "\tau" in the paper
	CrossShardEdgeNum int            // Total number of cross-shard edges
	ShardNum          int            // Number of shards
	GraphHash         []byte
}

func (graph *CLPAState) Hash() []byte {
	hash := sha256.Sum256(graph.Encode())
	return hash[:]
}

func (graph *CLPAState) Encode() []byte {
	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(graph)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

// Add a node, it needs to be assigned to a shard by default
func (cs *CLPAState) AddVertex(v Vertex) {
	cs.NetGraph.AddVertex(v)
	if val, ok := cs.PartitionMap[v]; !ok {
		cs.PartitionMap[v] = utils.Addr2Shard(v.Addr)
	} else {
		cs.PartitionMap[v] = val
	}
	cs.VertexsNumInShard[cs.PartitionMap[v]] += 1 // This can be modified after batch processing the VertexsNumInShard parameter
	// Alternatively, it can be left unprocessed since the CLPA algorithm will update the latest parameters before running
}

// Add an edge, the endpoints (if they do not exist) need to be assigned to a shard by default
func (cs *CLPAState) AddEdge(u, v Vertex) {
	// If the node doesn't exist, add it. The weight is always 1.
	if _, ok := cs.NetGraph.VertexSet[u]; !ok {
		cs.AddVertex(u)
	}
	if _, ok := cs.NetGraph.VertexSet[v]; !ok {
		cs.AddVertex(v)
	}
	cs.NetGraph.AddEdge(u, v)
	// Parameters like Edges2Shard can be modified after batch processing
	// Alternatively, it can be left unprocessed since the CLPA algorithm will update the latest parameters before running
}

// Copy CLPA state
func (dst *CLPAState) CopyCLPA(src CLPAState) {
	dst.NetGraph.CopyGraph(src.NetGraph)
	dst.PartitionMap = make(map[Vertex]int)
	for v := range src.PartitionMap {
		dst.PartitionMap[v] = src.PartitionMap[v]
	}
	dst.Edges2Shard = make([]int, src.ShardNum)
	copy(dst.Edges2Shard, src.Edges2Shard)
	dst.VertexsNumInShard = src.VertexsNumInShard
	dst.WeightPenalty = src.WeightPenalty
	dst.MinEdges2Shard = src.MinEdges2Shard
	dst.MaxIterations = src.MaxIterations
	dst.ShardNum = src.ShardNum
}

// Print CLPA
func (cs *CLPAState) PrintCLPA() {
	cs.NetGraph.PrintGraph()
	println(cs.MinEdges2Shard)
	for v, item := range cs.PartitionMap {
		print(v.Addr, " ", item, "\t")
	}
	for _, item := range cs.Edges2Shard {
		print(item, " ")
	}
	println()
}

// Calculate Wk, i.e., Edges2Shard, based on the current partition
func (cs *CLPAState) ComputeEdges2Shard() {
	cs.Edges2Shard = make([]int, cs.ShardNum)
	interEdge := make([]int, cs.ShardNum)
	cs.MinEdges2Shard = math.MaxInt

	for idx := 0; idx < cs.ShardNum; idx++ {
		cs.Edges2Shard[idx] = 0
		interEdge[idx] = 0
	}

	for v, lst := range cs.NetGraph.EdgeSet {
		// Get the shard to which node v belongs
		vShard := cs.PartitionMap[v]
		for _, u := range lst {
			// Similarly, get the shard to which node u belongs
			uShard := cs.PartitionMap[u]
			if vShard != uShard {
				// If nodes v and u do not belong to the same shard, increment the corresponding Edges2Shard by one
				// Only calculate the in-degree to avoid double counting
				cs.Edges2Shard[uShard] += 1
			} else {
				interEdge[uShard]++
			}
		}
	}

	cs.CrossShardEdgeNum = 0
	for _, val := range cs.Edges2Shard {
		cs.CrossShardEdgeNum += val
	}
	cs.CrossShardEdgeNum /= 2

	for idx := 0; idx < cs.ShardNum; idx++ {
		cs.Edges2Shard[idx] += interEdge[idx] / 2
	}
	// Update MinEdges2Shard and CrossShardEdgeNum
	for _, val := range cs.Edges2Shard {
		if cs.MinEdges2Shard > val {
			cs.MinEdges2Shard = val
		}
	}
}

// Recalculate parameters when the shard of an account changes, faster
func (cs *CLPAState) changeShardRecompute(v Vertex, old int) {
	new := cs.PartitionMap[v]
	for _, u := range cs.NetGraph.EdgeSet[v] {
		neighborShard := cs.PartitionMap[u]
		if neighborShard != new && neighborShard != old {
			cs.Edges2Shard[new]++
			cs.Edges2Shard[old]--
		} else if neighborShard == new {
			cs.Edges2Shard[old]--
			cs.CrossShardEdgeNum--
		} else {
			cs.Edges2Shard[new]++
			cs.CrossShardEdgeNum++
		}
	}
	cs.MinEdges2Shard = math.MaxInt
	// Update MinEdges2Shard and CrossShardEdgeNum
	for _, val := range cs.Edges2Shard {
		if cs.MinEdges2Shard > val {
			cs.MinEdges2Shard = val
		}
	}
}

// Set parameters
func (cs *CLPAState) Init_CLPAState(wp float64, mIter, sn int) {
	cs.WeightPenalty = wp
	cs.MaxIterations = mIter
	cs.ShardNum = sn
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
}

// Initialize partition, using the last digits of the node address, ensuring no empty shards at initialization
func (cs *CLPAState) Init_Partition() {
	// Set default partition parameters
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	for v := range cs.NetGraph.VertexSet {
		var va = v.Addr[len(v.Addr)-8:]
		num, err := strconv.ParseInt(va, 16, 64)
		if err != nil {
			log.Panic()
		}
		cs.PartitionMap[v] = int(num) % cs.ShardNum
		cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
	}
	cs.ComputeEdges2Shard() // Removing this will be faster, but this facilitates output (after all, Init is only executed once, so it won't be much faster)
}

// Initialize partition without empty shards
func (cs *CLPAState) Stable_Init_Partition() error {
	// Set default partition parameters
	if cs.ShardNum > len(cs.NetGraph.VertexSet) {
		return errors.New("too many shards, number of shards should be less than nodes")
	}
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	cnt := 0
	// Assign nodes to shards
	// NOTE: The order of nodes in the map is random, so the assignment will also be random.
	// Nodes are assigned in sequence starting from 0 using a counter (cnt), and the shard number is determined by cnt % ShardNum.
	// This ensures that nodes are evenly distributed across the shards.
	for v := range cs.NetGraph.VertexSet {
		cs.PartitionMap[v] = int(cnt) % cs.ShardNum
		cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
		cnt++
	}
	cs.ComputeEdges2Shard() // Removing this will be faster, but this facilitates output (after all, Init is only executed once, so it won't be much faster)
	return nil
}

// Calculate the score of placing node v into uShard
func (cs *CLPAState) getShard_score(v Vertex, uShard int) float64 {
	var score float64
	// Out-degree of node v
	v_outdegree := len(cs.NetGraph.EdgeSet[v])
	// Number of edges connecting uShard and node v
	Edgesto_uShard := 0
	for _, item := range cs.NetGraph.EdgeSet[v] {
		if cs.PartitionMap[item] == uShard {
			Edgesto_uShard += 1
		}
	}
	score = float64(Edgesto_uShard) / float64(v_outdegree) * (1 - cs.WeightPenalty*float64(cs.Edges2Shard[uShard])/float64(cs.MinEdges2Shard))
	return score
}

// CLPA partitioning algorithm
func (cs *CLPAState) CLPA_Partition() (map[string]uint64, int) {
	cs.ComputeEdges2Shard()
	fmt.Println(cs.CrossShardEdgeNum)
	res := make(map[string]uint64)
	updateTreshold := make(map[string]int)
	for iter := 0; iter < cs.MaxIterations; iter += 1 { // The outer loop controls the number of iterations, constraint
		for v := range cs.NetGraph.VertexSet {
			if updateTreshold[v.Addr] >= 50 {
				continue
			}
			neighborShardScore := make(map[int]float64)
			max_score := -9999.0
			vNowShard, max_scoreShard := cs.PartitionMap[v], cs.PartitionMap[v]
			for _, u := range cs.NetGraph.EdgeSet[v] {
				uShard := cs.PartitionMap[u]
				// For neighbors belonging to uShard, only calculate once
				if _, computed := neighborShardScore[uShard]; !computed {
					neighborShardScore[uShard] = cs.getShard_score(v, uShard)
					if max_score < neighborShardScore[uShard] {
						max_score = neighborShardScore[uShard]
						max_scoreShard = uShard
					}
				}
			}
			if vNowShard != max_scoreShard && cs.VertexsNumInShard[vNowShard] > 1 {
				cs.PartitionMap[v] = max_scoreShard
				res[v.Addr] = uint64(max_scoreShard)
				updateTreshold[v.Addr]++
				// Recalculate VertexsNumInShard
				cs.VertexsNumInShard[vNowShard] -= 1
				cs.VertexsNumInShard[max_scoreShard] += 1
				// Recalculate Wk
				cs.changeShardRecompute(v, vNowShard)
			}
		}
	}
	for sid, n := range cs.VertexsNumInShard {
		fmt.Printf("%d has vertexs: %d\n", sid, n)
	}

	cs.ComputeEdges2Shard()
	fmt.Println(cs.CrossShardEdgeNum)
	return res, cs.CrossShardEdgeNum
}

func (cs *CLPAState) EraseEdges() {
	cs.NetGraph.EdgeSet = make(map[Vertex][]Vertex)
}
