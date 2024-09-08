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

var predefinedColors = []string{
	"red", "green", "blue", "yellow", "purple", "orange", "pink",
	"cyan", "brown", "magenta", "lime", "indigo", "violet",
	// 必要に応じてさらに色を追加
}

// シャード番号に応じて色を取得する関数
func getColorForShard(shard int) string {
	return predefinedColors[shard%len(predefinedColors)] // シャード数が色の数を超えたら循環させる
}

// グラフを.dotファイルに出力する関数（スマートコントラクトと通常アカウントの視覚的区別付き）
func writeGraphToDotFile(filename string, clpaState CLPAState, contractAddrs map[string]bool) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fmt.Fprintln(file, "graph G {")

	// ノードごとの色設定とラベル表示
	for v, shard := range clpaState.PartitionMap {
		label := v.Addr[len(v.Addr)-3:]
		color := getColorForShard(shard) // シャード番号に応じて色を取得

		// スマートコントラクトかどうかを確認
		if contractAddrs[v.Addr] {
			// スマートコントラクトは四角で表示
			fmt.Fprintf(file, "    \"%s\" [label=\"%s\", color=\"%s\", shape=box, style=filled];\n", v.Addr, label, color)
		} else {
			// 通常アカウントは丸で表示
			fmt.Fprintf(file, "    \"%s\" [label=\"%s\", color=\"%s\", shape=circle, style=filled];\n", v.Addr, label, color)
		}
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
	for i := 0; i < ShardNum; i++ { // シャード数に応じて動的に生成
		color := getColorForShard(i)
		fmt.Fprintf(file, "        shard%d [label=\"Shard %d\", shape=box, style=filled, color=\"%s\"];\n", i, i, color)
	}
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

// ノードを作成し、エッジを追加する関数（スマートコントラクトの判断を含む）
func createGraphFromCSV(blockTxFile, internalTxFile string) (Graph, map[string]bool, error) {
	// グラフの初期化
	graph := Graph{
		VertexSet: make(map[Vertex]bool),
		EdgeSet:   make(map[Vertex][]Vertex),
	}

	// スマートコントラクトアドレスを保持するマップ
	contractAddrs := make(map[string]bool)

	// Block Transaction CSVからノードとエッジを追加
	blockFile, err := os.Open(blockTxFile)
	if err != nil {
		return graph, contractAddrs, err
	}
	defer blockFile.Close()

	blockReader := csv.NewReader(blockFile)
	blockTxData, err := blockReader.ReadAll()
	if err != nil {
		return graph, contractAddrs, err
	}

	for _, row := range blockTxData[1:] { // Skip header
		if len(row) < 8 {
			fmt.Println("Invalid block transaction row, skipping:", row)
			continue // 不正なデータ行をスキップ
		}
		fromAddr := row[3]              // `from` カラム
		toAddr := row[4]                // `to` カラム
		fromIsContract := row[6] == "1" // `fromIsContract` カラム
		toIsContract := row[7] == "1"   // `toIsContract` カラム

		fromVertex := Vertex{Addr: fromAddr}
		toVertex := Vertex{Addr: toAddr}

		// スマートコントラクトをマップに追加
		if fromIsContract {
			contractAddrs[fromAddr] = true
		}
		if toIsContract {
			contractAddrs[toAddr] = true
		}

		// ノードを追加
		graph.AddVertex(fromVertex)
		graph.AddVertex(toVertex)

		// エッジを追加
		graph.AddEdge(fromVertex, toVertex)
	}

	// Internal Transaction CSVからノードとエッジを追加
	internalFile, err := os.Open(internalTxFile)
	if err != nil {
		return graph, contractAddrs, err
	}
	defer internalFile.Close()

	internalReader := csv.NewReader(internalFile)
	internalTxData, err := internalReader.ReadAll()
	if err != nil {
		return graph, contractAddrs, err
	}

	for _, row := range internalTxData[1:] { // Skip header
		if len(row) < 9 {
			fmt.Println("Invalid internal transaction row, skipping:", row)
			continue // 不正なデータ行をスキップ
		}
		fromAddr := row[4]              // `from` カラム
		toAddr := row[5]                // `to` カラム
		fromIsContract := row[6] == "1" // `fromIsContract` カラム
		toIsContract := row[7] == "1"   // `toIsContract` カラム

		fromVertex := Vertex{Addr: fromAddr}
		toVertex := Vertex{Addr: toAddr}

		// スマートコントラクトをマップに追加
		if fromIsContract {
			contractAddrs[fromAddr] = true
		}
		if toIsContract {
			contractAddrs[toAddr] = true
		}

		// ノードを追加
		graph.AddVertex(fromVertex)
		graph.AddVertex(toVertex)

		// エッジを追加
		graph.AddEdge(fromVertex, toVertex)
	}

	return graph, contractAddrs, nil
}

// メインテスト関数でグラフ作成を呼び出す
func TestCLPA_PartitionFromCSV(t *testing.T) {
	// CSVファイルからグラフを作成
	blockTxFile := "../20000000to20249999_BlockTransaction_subset.csv"
	internalTxFile := "../20000000to20249999_InternalTransaction_subset.csv"
	fmt.Println("Creating graph from CSV files...")
	graph, contractAddrs, err := createGraphFromCSV(blockTxFile, internalTxFile)
	if err != nil {
		t.Fatalf("Error creating graph: %v", err)
	}

	// スマートコントラクトアドレスの確認
	for addr := range contractAddrs {
		fmt.Println("Smart contract address:", addr)
	}

	// CLPAStateの初期化
	clpaState := CLPAState{
		NetGraph: graph,
	}
	clpaState.Init_CLPAState(WeightPenalty, MaxIterations, ShardNum) // WeightPenalty(beta), MaxIterations(tau), ShardNum

	// 初期シャード割り当て
	clpaState.Init_Partition()

	// 初期シャード割り当てをログに表示
	printInitialPartition(clpaState)

	// シャード分割を実行
	fmt.Println("シャード分割を実行")
	_, crossShardEdgeNum := clpaState.CLPA_Partition()
	fmt.Println("シャード分割完了")

	// シャード分割後の結果を表示
	printFinalPartition(clpaState, crossShardEdgeNum)

	// CLPA後のグラフを.dotファイルに書き出し（スマートコントラクトを考慮）
	writeGraphToDotFile("final_partition.dot", clpaState, contractAddrs)
}
