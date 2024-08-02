package partition

import (
	"fmt"
	"os"
	"testing"
)

// ノードを作成し、エッジを追加する関数
func createGraph() Graph {
	// グラフの初期化
	graph := Graph{
		VertexSet: make(map[Vertex]bool),
		EdgeSet:   make(map[Vertex][]Vertex),
	}

	// ノードの作成(shard数は3)
	nodes := []Vertex{
		{Addr: "00000000000000000000000000000000000000"}, //shard 0
		{Addr: "00000000000000000000000000000000000001"}, //shard 1
		{Addr: "00000000000000000000000000000000000002"}, //shard 2
		{Addr: "00000000000000000000000000000000000003"}, //shard 0
		{Addr: "00000000000000000000000000000000000004"}, //shard 1
		{Addr: "00000000000000000000000000000000000005"}, //shard 2
		{Addr: "00000000000000000000000000000000000006"}, //shard 0
		{Addr: "00000000000000000000000000000000000007"}, //shard 1
		{Addr: "00000000000000000000000000000000000008"}, //shard 2
		{Addr: "00000000000000000000000000000000000009"}, //shard 0
	}

	// ノードをグラフに追加
	for _, node := range nodes {
		graph.AddVertex(node)
	}

	// エッジを追加
	edges := [][2]int{
		{0, 1}, {0, 2}, {1, 2}, {2, 3}, {3, 4},
		{4, 5}, {5, 6}, {6, 7}, {7, 8}, {8, 9},
		{0, 5}, {1, 6}, {2, 7}, {3, 8}, {4, 9},
	}
	for _, edge := range edges {
		graph.AddEdge(nodes[edge[0]], nodes[edge[1]])
	}

	return graph
}

// グラフを.dotファイルに出力する関数
func writeGraphToDotFile(filename string, clpaState CLPAState) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fmt.Fprintln(file, "graph G {")

	// ノードごとの色設定とラベル表示
	colors := []string{"red", "green", "blue"}
	for v, shard := range clpaState.PartitionMap {
		label := v.Addr[len(v.Addr)-3:]
		fmt.Fprintf(file, "    \"%s\" [label=\"%s\", color=%s, style=filled];\n", v.Addr, label, colors[shard])
	}

	for v, neighbors := range clpaState.NetGraph.EdgeSet {
		for _, u := range neighbors {
			if v.Addr < u.Addr {
				fmt.Fprintf(file, "    \"%s\" -- \"%s\";\n", v.Addr, u.Addr)
			}
		}
	}

	// 凡例を追加
	fmt.Fprintln(file, "    subgraph cluster_legend {")
	fmt.Fprintln(file, "        label = \"Shard Legend\";")
	for i, color := range colors {
		fmt.Fprintf(file, "        shard%d [label=\"Shard %d\", shape=box, style=filled, color=%s];\n", i, i, color)
	}
	fmt.Fprintln(file, "        shard0 -- shard1 [style=invis];")
	fmt.Fprintln(file, "        shard1 -- shard2 [style=invis];")
	fmt.Fprintln(file, "    }")

	fmt.Fprintln(file, "}")
}


// 初期シャード割り当てを表示する関数
func printInitialPartition(clpaState CLPAState) {
	fmt.Println("初期のシャードの割り当て")
	for v, shard := range clpaState.PartitionMap {
		fmt.Printf("Node %s is in shard %d\n", v.Addr, shard)
	}
	fmt.Printf("初期Edges2Shard: %v\n", clpaState.Edges2Shard)
	fmt.Printf("初期CrossShardEdgeNum: %d\n\n", clpaState.CrossShardEdgeNum)
}

// シャード分割後の結果を表示する関数
func printFinalPartition(clpaState CLPAState, crossShardEdgeNum int) {
	fmt.Println("CLPAのシャードの割り当て適用")
	for v, shard := range clpaState.PartitionMap {
		fmt.Printf("Node %s is in shard %d\n", v.Addr, shard)
	}
	fmt.Printf("CLPA後Edges2Shard: %v\n", clpaState.Edges2Shard)
	fmt.Printf("CLPA後CrossShardEdgeNum: %d\n\n", crossShardEdgeNum)
}

func TestCLPA_Partition(t *testing.T) {
	// グラフの作成
	graph := createGraph()

	// CLPAStateの初期化
	clpaState := CLPAState{
		NetGraph: graph,
	}
	clpaState.Init_CLPAState(0.5, 100, 3) // WeightPenalty(beta), MaxIterations(tau), ShardNum

	// 初期シャード割り当て
	clpaState.Init_Partition()

	// 初期シャード割り当てをログに表示
	printInitialPartition(clpaState)

	// 初期グラフを.dotファイルに書き出し
	writeGraphToDotFile("initial_partition.dot", clpaState)

	// シャード分割を実行
	fmt.Println("シャード分割を実行")
	_, crossShardEdgeNum := clpaState.CLPA_Partition()
	fmt.Println("シャード分割完了")

	// シャード分割後の結果を表示
	printFinalPartition(clpaState, crossShardEdgeNum)

	// CLPA後のグラフを.dotファイルに書き出し
	writeGraphToDotFile("final_partition.dot", clpaState)
}
