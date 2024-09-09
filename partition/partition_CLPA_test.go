package partition

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"testing"
)

// 定数
const (
	WeightPenalty           = 0.5
	MaxIterations           = 100
	ShardNum                = 3
	AccountGraphShape       = "circle"
	SmartContractGraphShape = "box"
	IsLoadInternalTx        = true
	IsUseContractAccounts   = true // スマートコントラクトアカウントを含むデータをスキップ
	BlockTxFilePath         = "../20000000to20249999_BlockTransaction.csv"
	ReadBlockNumber         = 20000050
	InternalTxFilePath      = "../20000000to20249999_InternalTransaction_1000000rows.csv"
)

// shardの色を定義(67色)
var predefinedColors = []string{
	"red", "green", "blue", "yellow", "purple", "orange", "pink",
	"cyan", "brown", "magenta", "lime", "indigo", "violet",
	"gold", "silver", "coral", "turquoise", "teal", "navy",
	"olive", "maroon", "salmon", "khaki", "plum", "orchid",
	"lavender", "beige", "mint", "chocolate", "crimson", "periwinkle",
	"peach", "apricot", "amethyst", "skyblue", "lightgreen", "aquamarine",
	"sienna", "ivory", "tan", "forestgreen", "steelblue", "slategray",
	"lightcoral", "darkcyan", "deepskyblue", "firebrick", "fuchsia", "darkgoldenrod",
	"lightseagreen", "midnightblue", "rosybrown", "dodgerblue", "darkorchid", "palegoldenrod",
	"springgreen", "tomato", "wheat", "lemonchiffon", "darkolivegreen", "mediumaquamarine",
	"hotpink", "papayawhip", "darkseagreen", "lightpink", "royalblue", "seagreen",
}

// グラフの色情報を取得
func getColorForShard(shard int) string {
	return predefinedColors[shard%len(predefinedColors)]
}

// .dotファイルへの出力
func writeGraphToDotFile(filename string, clpaState CLPAState, contractAddrs map[string]bool) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	fmt.Fprintln(file, "graph G {")
	writeNodes(file, clpaState, contractAddrs)
	writeEdges(file, clpaState)
	writeLegend(file)
	fmt.Fprintln(file, "}")
	return nil
}

// ノード情報を書き込む
func writeNodes(file *os.File, clpaState CLPAState, contractAddrs map[string]bool) {
	for v, shard := range clpaState.PartitionMap {
		label := v.Addr[len(v.Addr)-3:]
		color := getColorForShard(shard)
		shape := AccountGraphShape
		if contractAddrs[v.Addr] {
			shape = SmartContractGraphShape
		}
		fmt.Fprintf(file, "    \"%s\" [label=\"%s\", color=\"%s\", shape=%s, style=filled];\n", v.Addr, label, color, shape)
	}
}

// エッジ情報を書き込む
func writeEdges(file *os.File, clpaState CLPAState) {
	for v, neighbors := range clpaState.NetGraph.EdgeSet {
		for _, u := range neighbors {
			if v.Addr < u.Addr {
				fmt.Fprintf(file, "    \"%s\" -- \"%s\";\n", v.Addr, u.Addr)
			}
		}
	}
}

// 凡例を書き込む
func writeLegend(file *os.File) {
	fmt.Fprintln(file, "    subgraph cluster_legend {")
	fmt.Fprintln(file, "        label = \"Shard Legend\";")
	for i := 0; i < ShardNum; i++ {
		color := getColorForShard(i)
		fmt.Fprintf(file, "        shard%d [label=\"Shard %d\", shape=box, style=filled, color=\"%s\"];\n", i, i, color)
	}
	fmt.Fprintln(file, "    }")
}

// ノードとエッジを追加
func processTxData(data [][]string, graph *Graph, contractAddrs map[string]bool) {
	for _, row := range data {
		fromAddr, toAddr := row[3], row[4]
		fromIsContract := row[6] == "1"
		toIsContract := row[7] == "1"

		if IsUseContractAccounts {
			if fromIsContract {
				contractAddrs[fromAddr] = true
			}
			if toIsContract {
				contractAddrs[toAddr] = true
			}
		}
		addEdgeToGraph(graph, fromAddr, toAddr)
	}
}

// ノードとエッジを追加
func processInternalTxData(data [][]string, graph *Graph, contractAddrs map[string]bool) {
	for _, row := range data {
		fromAddr, toAddr := row[4], row[5]
		fromIsContract := row[6] == "1"
		toIsContract := row[7] == "1"

		if IsUseContractAccounts {
			if fromIsContract {
				contractAddrs[fromAddr] = true
			}
			if toIsContract {
				contractAddrs[toAddr] = true
			}
		}
		addEdgeToGraph(graph, fromAddr, toAddr)
	}
}

// グラフにエッジを追加
func addEdgeToGraph(graph *Graph, fromAddr, toAddr string) {
	fromVertex := Vertex{Addr: fromAddr}
	toVertex := Vertex{Addr: toAddr}
	graph.AddVertex(fromVertex)
	graph.AddVertex(toVertex)
	graph.AddEdge(fromVertex, toVertex)
}

// CSVファイルを blockNumber に基づいて読み込む関数
func readTxCSVUntilBlock(filename string, maxBlockNumber int) ([][]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, _ = reader.Read() // ヘッダーをスキップ

	var data [][]string
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}

		// blockNumber が maxBlockNumber を超えたら読み込み終了
		blockNumber := parseBlockNumber(row[0]) // blockNumber が1列目にあると仮定
		if blockNumber > maxBlockNumber {
			break
		}

		data = append(data, row)
	}
	return data, nil
}

// blockNumber をパースするためのヘルパー関数
func parseBlockNumber(blockNumberStr string) int {
	blockNumber, _ := strconv.Atoi(blockNumberStr) // エラーは無視するか、エラーハンドリングを追加
	return blockNumber
}

// InternalTransactionを blockNumber に基づいて読み込む関数
func readInternalTxCSVUntilBlock(internalTxFile string, maxBlockNumber int) ([][]string, error) {
	file, err := os.Open(internalTxFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, _ = reader.Read() // ヘッダーをスキップ

	var data [][]string
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}

		// blockNumber が maxBlockNumber を超えたら読み込み終了
		blockNumber := parseBlockNumber(row[1]) // InternalTransactionで blockNumber が2列目にあると仮定
		if blockNumber > maxBlockNumber {
			break
		}

		data = append(data, row)
	}
	return data, nil
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

// CSVファイルからグラフを作成
func createGraphFromCSV(blockTxFilePath, internalTxFilePath string, maxBlockNumber int) (Graph, map[string]bool, error) {
	graph := Graph{
		VertexSet: make(map[Vertex]bool),
		EdgeSet:   make(map[Vertex][]Vertex),
	}
	contractAddrs := make(map[string]bool)

	// Block Transaction CSVの読み込み
	blockTxData, err := readTxCSVUntilBlock(blockTxFilePath, maxBlockNumber)
	if err != nil {
		return graph, contractAddrs, err
	}
	processTxData(blockTxData, &graph, contractAddrs)

	// Internal Transaction CSVの読み込み
	if IsLoadInternalTx {
		internalTxData, err := readInternalTxCSVUntilBlock(internalTxFilePath, maxBlockNumber)
		if err != nil {
			return graph, contractAddrs, err
		}
		processInternalTxData(internalTxData, &graph, contractAddrs)
	}

	return graph, contractAddrs, nil
}

// メインテスト関数
func TestCLPA_PartitionFromCSV(t *testing.T) {
	fmt.Println("Creating graph from CSV files...")
	graph, contractAddrs, err := createGraphFromCSV(BlockTxFilePath, InternalTxFilePath, ReadBlockNumber)
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
