package partition

import (
	"fmt"
	"testing"
)

func TestCLPA_Partition(t *testing.T) {
	// グラフの初期化
	graph := Graph{
		VertexSet: make(map[Vertex]bool),
		EdgeSet:   make(map[Vertex][]Vertex),
	}

	// ノードの作成
	nodes := []Vertex{
		{Addr: "A"},
		{Addr: "B"},
		{Addr: "C"},
		{Addr: "D"},
		{Addr: "E"},
	}

	// ノードをグラフに追加
	for _, node := range nodes {
		graph.AddVertex(node)
	}

	// エッジを追加 (A-B, A-C, B-C, C-D, D-E)→(0-1, 0-2, 1-2, 2-3, 3-4)
	graph.AddEdge(nodes[0], nodes[1])
	graph.AddEdge(nodes[0], nodes[2])
	graph.AddEdge(nodes[1], nodes[2])
	graph.AddEdge(nodes[2], nodes[3])
	graph.AddEdge(nodes[3], nodes[4])

	// CLPAStateの初期化
	clpaState := CLPAState{
		NetGraph: graph,
	}
	clpaState.Init_CLPAState(0.5, 10, 3) // WeightPenalty, MaxIterations, ShardNum

	// 初期シャード割り当ての確認用
	err := clpaState.Stable_Init_Partition()
	if err != nil {
		t.Fatalf("Failed to initialize partition: %v", err)
	}

	t.Log("Initial Partition!:")
	clpaState.PrintCLPA()
	t.Log(clpaState.PartitionMap)
	for v, shard := range clpaState.PartitionMap {
		fmt.Printf("Node %s is in shard %d\n", v.Addr, shard)
	}

	// シャード分割を実行
	_, crossShardEdgeNum := clpaState.CLPA_Partition()

	// 結果の確認
	for v, shard := range clpaState.PartitionMap {
		fmt.Printf("Node %s is in shard %d\n", v.Addr, shard)
	}

	// 期待される結果を定義
	/* expectedShards := map[string]int{
		"A": 1,
		"B": 1,
		"C": 0,
		"D": 0,
		"E": 0,
	} */
	expectedCrossShardEdges := 3 // 期待されるシャード間エッジ数

	// 結果を検証
	/* for addr, shard := range expectedShards {
		if result[addr] != uint64(shard) {
			t.Errorf("Node %s expected in shard %d, but got %d", addr, shard, result[addr])
		}
	} */

	if crossShardEdgeNum != expectedCrossShardEdges {
		t.Errorf("Expected %d cross-shard edges, but got %d", expectedCrossShardEdges, crossShardEdgeNum)
	}

	t.Log(clpaState.Edges2Shard)
}
