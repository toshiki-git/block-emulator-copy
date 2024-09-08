package partition

import (
	"encoding/csv"
	"fmt"
	"os"
	"testing"
)

const (
	WeightPenalty = 0.5
	MaxIterations = 100
	ShardNum      = 10
)

var predefinedColors = []string{
	"red", "green", "blue", "yellow", "purple", "orange", "pink",
	"cyan", "brown", "magenta", "lime", "indigo", "violet",
}

// シャード番号に応じて色を取得する関数
func getColorForShard(shard int) string {
	return predefinedColors[shard%len(predefinedColors)] // シャード数が色の数を超えたら循環させる
}

// グラフを.dotファイルに出力する関数（スマートコントラクトと通常アカウントの視覚的区別付き）
func writeGraphToDotFile(filename string, clpaState CLPAState, contractAddrs map[string]bool) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	fmt.Fprintln(file, "graph G {")

	// ノードごとの色設定とラベル表示
	for v, shard := range clpaState.PartitionMap {
		label := v.Addr[len(v.Addr)-3:]
		color := getColorForShard(shard)

		shape := "circle" // デフォルトは丸
		if contractAddrs[v.Addr] {
			shape = "box" // スマートコントラクトの場合は四角
		}
		fmt.Fprintf(file, "    \"%s\" [label=\"%s\", color=\"%s\", shape=%s, style=filled];\n", v.Addr, label, color, shape)
	}

	// エッジを追加
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
	for i := 0; i < ShardNum; i++ {
		color := getColorForShard(i)
		fmt.Fprintf(file, "        shard%d [label=\"Shard %d\", shape=box, style=filled, color=\"%s\"];\n", i, i, color)
	}
	fmt.Fprintln(file, "    }")
	fmt.Fprintln(file, "}")
	return nil
}

// 初期シャード割り当てを表示する関数
func printPartition(clpaState CLPAState, label string) {
	fmt.Printf("%sのシャードの割り当て:\n", label)
	for v, shard := range clpaState.PartitionMap {
		fmt.Printf("Node %s is in shard %d\n", v.Addr, shard)
	}
	fmt.Printf("Edges2Shard: %v\n", clpaState.Edges2Shard)
	fmt.Printf("CrossShardEdgeNum: %d\n\n", clpaState.CrossShardEdgeNum)
}

// ノードとエッジを追加する共通関数
func processTransactionData(data [][]string, graph *Graph, contractAddrs map[string]bool, fromIdx, toIdx, fromIsContractIdx, toIsContractIdx int, useContractAccounts bool) {
	for _, row := range data[1:] { // Skip header
		if len(row) < toIdx+1 {
			fmt.Println("Invalid transaction row, skipping:", row)
			continue
		}
		fromAddr := row[fromIdx]
		toAddr := row[toIdx]

		if useContractAccounts {
			fromIsContract := row[fromIsContractIdx] == "1"
			toIsContract := row[toIsContractIdx] == "1"
			if fromIsContract {
				contractAddrs[fromAddr] = true
			}
			if toIsContract {
				contractAddrs[toAddr] = true
			}
		}

		fromVertex := Vertex{Addr: fromAddr}
		toVertex := Vertex{Addr: toAddr}

		graph.AddVertex(fromVertex)
		graph.AddVertex(toVertex)
		graph.AddEdge(fromVertex, toVertex)
	}
}

// CSVファイルからグラフを作成
func createGraphFromCSV(blockTxFile, internalTxFile string, loadInternalTx, useContractAccounts bool) (Graph, map[string]bool, error) {
	graph := Graph{
		VertexSet: make(map[Vertex]bool),
		EdgeSet:   make(map[Vertex][]Vertex),
	}
	contractAddrs := make(map[string]bool)

	// Block Transaction CSVの読み込み
	blockTxData, err := readCSV(blockTxFile)
	if err != nil {
		return graph, contractAddrs, err
	}
	processTransactionData(blockTxData, &graph, contractAddrs, 3, 4, 6, 7, useContractAccounts)

	// Internal Transaction CSVの読み込み (loadInternalTxがtrueの場合のみ)
	if loadInternalTx {
		internalTxData, err := readCSV(internalTxFile)
		if err != nil {
			return graph, contractAddrs, err
		}
		processTransactionData(internalTxData, &graph, contractAddrs, 4, 5, 6, 7, useContractAccounts)
	}

	return graph, contractAddrs, nil
}

// CSVファイルを読み込む関数
func readCSV(filename string) ([][]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	return reader.ReadAll()
}

// メインテスト関数でグラフ作成を呼び出す
func TestCLPA_PartitionFromCSV(t *testing.T) {
	blockTxFile := "../20000000to20249999_BlockTransaction_subset.csv"
	internalTxFile := "../20000000to20249999_InternalTransaction_subset.csv"
	loadInternalTx := true      // Internal TXの読み込みを制御
	useContractAccounts := true // スマートコントラクトアカウントを無視

	fmt.Println("Creating graph from CSV files...")
	graph, contractAddrs, err := createGraphFromCSV(blockTxFile, internalTxFile, loadInternalTx, useContractAccounts)
	if err != nil {
		t.Fatalf("Error creating graph: %v", err)
	}

	for addr := range contractAddrs {
		fmt.Println("Smart contract address:", addr)
	}

	clpaState := CLPAState{NetGraph: graph}
	clpaState.Init_CLPAState(WeightPenalty, MaxIterations, ShardNum)

	clpaState.Init_Partition()
	printPartition(clpaState, "初期")

	fmt.Println("シャード分割を実行")
	clpaState.CLPA_Partition()
	fmt.Println("シャード分割完了")

	printPartition(clpaState, "CLPA後")

	err = writeGraphToDotFile("final_partition.dot", clpaState, contractAddrs)
	if err != nil {
		t.Fatalf("Error writing .dot file: %v", err)
	}
}

// ノードを作成し、エッジを追加する関数
/* func createGraph() Graph {
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
} */

/* func TestCLPA_Partition(t *testing.T) {
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
} */
