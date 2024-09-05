package params

import "math/big"

type ChainConfig struct {
	ChainID        uint64
	NodeID         uint64
	ShardID        uint64
	Nodes_perShard uint64
	ShardNums      uint64
	BlockSize      uint64
	BlockInterval  uint64
	InjectSpeed    uint64

	// used in transaction relaying, useless in brokerchain mechanism
	MaxRelayBlockSize uint64
}

var (
	DeciderShard     = uint64(0xffffffff) // Supervisor ShardID
	Init_Balance, _  = new(big.Int).SetString("100000000000000000000000000000000000000000000", 10)
	IPmap_nodeTable  = make(map[uint64]map[uint64]string) //IPmap_nodeTable[shardID][nodeID] = "IP:Port"
	CommitteeMethod  = []string{"CLPA_Broker", "CLPA", "Broker", "Relay", "Proposal"}
	MeasureBrokerMod = []string{"TPS_Broker", "TCL_Broker", "CrossTxRate_Broker", "TxNumberCount_Broker"}
	MeasureRelayMod  = []string{"TPS_Relay", "TCL_Relay", "CrossTxRate_Relay", "TxNumberCount_Relay"}
)
