package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"
)

type ProposalPbftInsideExtraHandleMod struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
}

// propose request with different types
func (pphm *ProposalPbftInsideExtraHandleMod) HandleinPropose() (bool, *message.Request) {
	if pphm.cdm.PartitionOn {
		pphm.sendPartitionReady()
		for !pphm.getPartitionReady() {
			time.Sleep(time.Second)
		}
		// send accounts and txs
		pphm.sendAccounts_and_Txs()
		// propose a partition
		for !pphm.getCollectOver() {
			time.Sleep(time.Second)
		}
		return pphm.proposePartition()
	}

	// ELSE: propose a block
	block := pphm.pbftNode.CurChain.GenerateBlock()
	r := &message.Request{
		RequestType: message.BlockRequest,
		ReqTime:     time.Now(),
	}
	r.Msg.Content = block.Encode()
	return true, r

}

// the diy operation in preprepare
func (pphm *ProposalPbftInsideExtraHandleMod) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {
	// judge whether it is a partitionRequest or not
	isPartitionReq := ppmsg.RequestMsg.RequestType == message.PartitionReq

	if isPartitionReq {
		// after some checking
		pphm.pbftNode.pl.Plog.Printf("S%dN%d : a partition block\n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID)
	} else {
		// the request is a block
		if pphm.pbftNode.CurChain.IsValidBlock(core.DecodeB(ppmsg.RequestMsg.Msg.Content)) != nil {
			pphm.pbftNode.pl.Plog.Printf("S%dN%d : not a valid block\n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID)
			return false
		}
	}
	pphm.pbftNode.pl.Plog.Printf("S%dN%d : the pre-prepare message is correct, putting it into the RequestPool. \n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID)
	pphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
	// merge to be a prepare message
	return true
}

// the operation in prepare, and in pbft + tx relaying, this function does not need to do any.
func (pphm *ProposalPbftInsideExtraHandleMod) HandleinPrepare(pmsg *message.Prepare) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation in commit.
func (pphm *ProposalPbftInsideExtraHandleMod) HandleinCommit(cmsg *message.Commit) bool {
	r := pphm.pbftNode.requestPool[string(cmsg.Digest)]
	// requestType ...
	if r.RequestType == message.PartitionReq {
		// if a partition Requst ...
		atm := message.DecodeAccountTransferMsg(r.Msg.Content)
		pphm.accountTransfer_do(atm)
		return true
	}
	// if a block request ...
	block := core.DecodeB(r.Msg.Content)
	pphm.pbftNode.pl.Plog.Printf("S%dN%d : adding the block %d...now height = %d \n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID, block.Header.Number, pphm.pbftNode.CurChain.CurrentBlock.Header.Number)
	pphm.pbftNode.CurChain.AddBlock(block)
	pphm.pbftNode.pl.Plog.Printf("S%dN%d : added the block %d... \n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID, block.Header.Number)
	pphm.pbftNode.CurChain.PrintBlockChain()

	// now try to relay txs to other shards (for main nodes)
	if pphm.pbftNode.NodeID == pphm.pbftNode.view {
		pphm.pbftNode.pl.Plog.Printf("S%dN%d : main node is trying to send relay txs at height = %d \n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID, block.Header.Number)
		// generate relay pool and collect txs excuted
		pphm.pbftNode.CurChain.Txpool.RelayPool = make(map[uint64][]*core.Transaction)
		//innerかもしれない
		interShardTxs := make([]*core.Transaction, 0)
		relay1Txs := make([]*core.Transaction, 0)
		relay2Txs := make([]*core.Transaction, 0)

		for _, tx := range block.Body {
			ssid := pphm.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
			rsid := pphm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient)
			if !tx.Relayed && ssid != pphm.pbftNode.ShardID {
				log.Panic("incorrect tx")
			}
			if tx.Relayed && rsid != pphm.pbftNode.ShardID {
				log.Panic("incorrect tx")
			}
			if rsid != pphm.pbftNode.ShardID {
				relay1Txs = append(relay1Txs, tx)
				tx.Relayed = true
				pphm.pbftNode.CurChain.Txpool.AddRelayTx(tx, rsid)
			} else {
				if tx.Relayed {
					relay2Txs = append(relay2Txs, tx)
				} else {
					//innerかもしれない
					interShardTxs = append(interShardTxs, tx)
				}
			}
		}

		// send relay txs
		for sid := uint64(0); sid < pphm.pbftNode.pbftChainConfig.ShardNums; sid++ {
			if sid == pphm.pbftNode.ShardID {
				continue
			}
			relay := message.Relay{
				Txs:           pphm.pbftNode.CurChain.Txpool.RelayPool[sid],
				SenderShardID: pphm.pbftNode.ShardID,
				SenderSeq:     pphm.pbftNode.sequenceID,
			}
			rByte, err := json.Marshal(relay)
			if err != nil {
				log.Panic()
			}
			msg_send := message.MergeMessage(message.CRelay, rByte)
			go networks.TcpDial(msg_send, pphm.pbftNode.ip_nodeTable[sid][0])
			pphm.pbftNode.pl.Plog.Printf("S%dN%d : sended relay txs to %d\n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID, sid)
		}
		pphm.pbftNode.CurChain.Txpool.ClearRelayPool()

		// send txs excuted in this block to the listener
		// add more message to measure more metrics
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   interShardTxs,
			Epoch:           int(pphm.cdm.AccountTransferRound),

			Relay1Txs: relay1Txs,
			Relay2Txs: relay2Txs,

			SenderShardID: pphm.pbftNode.ShardID,
			ProposeTime:   r.ReqTime,
			CommitTime:    time.Now(),
		}
		bByte, err := json.Marshal(bim)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CBlockInfo, bByte)
		go networks.TcpDial(msg_send, pphm.pbftNode.ip_nodeTable[params.DeciderShard][0])
		pphm.pbftNode.pl.Plog.Printf("S%dN%d : sended excuted txs\n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID)

		pphm.pbftNode.CurChain.Txpool.GetLocked()

		metricName := []string{
			"Block Height",
			"EpochID of this block",
			"TxPool Size",
			"# of all Txs in this block",
			"# of Relay1 Txs in this block",
			"# of Relay2 Txs in this block",
			"TimeStamp - Propose (unixMill)",
			"TimeStamp - Commit (unixMill)",

			"SUM of confirm latency (ms, All Txs)",
			"SUM of confirm latency (ms, Relay1 Txs) (Duration: Relay1 proposed -> Relay1 Commit)",
			"SUM of confirm latency (ms, Relay2 Txs) (Duration: Relay1 proposed -> Relay2 Commit)",
		}
		metricVal := []string{
			strconv.Itoa(int(block.Header.Number)),
			strconv.Itoa(bim.Epoch),
			strconv.Itoa(len(pphm.pbftNode.CurChain.Txpool.TxQueue)),
			strconv.Itoa(len(block.Body)),
			strconv.Itoa(len(relay1Txs)),
			strconv.Itoa(len(relay2Txs)),
			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),

			strconv.FormatInt(computeTCL(block.Body, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay2Txs, bim.CommitTime), 10),
		}
		pphm.pbftNode.writeCSVline(metricName, metricVal)
		pphm.pbftNode.CurChain.Txpool.GetUnlocked()
	}
	return true
}

func (pphm *ProposalPbftInsideExtraHandleMod) HandleReqestforOldSeq(*message.RequestOldMessage) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation for sequential requests
func (pphm *ProposalPbftInsideExtraHandleMod) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	if int(som.SeqEndHeight-som.SeqStartHeight+1) != len(som.OldRequest) {
		pphm.pbftNode.pl.Plog.Printf("S%dN%d : the SendOldMessage message is not enough\n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID)
	} else { // add the block into the node pbft blockchain
		for height := som.SeqStartHeight; height <= som.SeqEndHeight; height++ {
			r := som.OldRequest[height-som.SeqStartHeight]
			if r.RequestType == message.BlockRequest {
				b := core.DecodeB(r.Msg.Content)
				pphm.pbftNode.CurChain.AddBlock(b)
			} else {
				atm := message.DecodeAccountTransferMsg(r.Msg.Content)
				pphm.accountTransfer_do(atm)
			}
		}
		pphm.pbftNode.sequenceID = som.SeqEndHeight + 1
		pphm.pbftNode.CurChain.PrintBlockChain()
	}
	return true
}

// account transfer happens when the leader received the re-partition message.
// leaders send the infos about the accounts to be transferred to other leaders, and
// handle them.
// this message used in propose stage, so it will be invoked by InsidePBFT_Module
func (pphm *ProposalPbftInsideExtraHandleMod) sendPartitionReady() {
	pphm.cdm.P_ReadyLock.Lock()
	pphm.cdm.PartitionReady[pphm.pbftNode.ShardID] = true
	pphm.cdm.P_ReadyLock.Unlock()

	pr := message.PartitionReady{
		FromShard: pphm.pbftNode.ShardID,
		NowSeqID:  pphm.pbftNode.sequenceID,
	}
	pByte, err := json.Marshal(pr)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionReady, pByte)
	for sid := 0; sid < int(pphm.pbftNode.pbftChainConfig.ShardNums); sid++ {
		if sid != int(pr.FromShard) {
			go networks.TcpDial(send_msg, pphm.pbftNode.ip_nodeTable[uint64(sid)][0])
		}
	}
	pphm.pbftNode.pl.Plog.Print("Ready for partition\n")
}

// get whether all shards is ready, it will be invoked by InsidePBFT_Module
func (pphm *ProposalPbftInsideExtraHandleMod) getPartitionReady() bool {
	pphm.cdm.P_ReadyLock.Lock()
	defer pphm.cdm.P_ReadyLock.Unlock()
	pphm.pbftNode.seqMapLock.Lock()
	defer pphm.pbftNode.seqMapLock.Unlock()
	pphm.cdm.ReadySeqLock.Lock()
	defer pphm.cdm.ReadySeqLock.Unlock()

	flag := true
	for sid, val := range pphm.pbftNode.seqIDMap {
		if rval, ok := pphm.cdm.ReadySeq[sid]; !ok || (rval-1 != val) {
			flag = false
		}
	}
	return len(pphm.cdm.PartitionReady) == int(pphm.pbftNode.pbftChainConfig.ShardNums) && flag
}

// send the transactions and the accountState to other leaders
func (pphm *ProposalPbftInsideExtraHandleMod) sendAccounts_and_Txs() {
	// generate accout transfer and txs message
	accountToFetch := make([]string, 0)
	lastMapid := len(pphm.cdm.ModifiedMap) - 1
	for key, val := range pphm.cdm.ModifiedMap[lastMapid] {
		if val != pphm.pbftNode.ShardID && pphm.pbftNode.CurChain.Get_PartitionMap(key) == pphm.pbftNode.ShardID {
			accountToFetch = append(accountToFetch, key)
		}
	}
	asFetched := pphm.pbftNode.CurChain.FetchAccounts(accountToFetch)
	// send the accounts to other shards
	pphm.pbftNode.CurChain.Txpool.GetLocked()
	pphm.pbftNode.pl.Plog.Println("The size of tx pool is: ", len(pphm.pbftNode.CurChain.Txpool.TxQueue))
	for i := uint64(0); i < pphm.pbftNode.pbftChainConfig.ShardNums; i++ {
		if i == pphm.pbftNode.ShardID {
			continue
		}
		addrSend := make([]string, 0)
		addrSet := make(map[string]bool)
		asSend := make([]*core.AccountState, 0)
		for idx, addr := range accountToFetch {
			if pphm.cdm.ModifiedMap[lastMapid][addr] == i {
				addrSend = append(addrSend, addr)
				addrSet[addr] = true
				asSend = append(asSend, asFetched[idx])
			}
		}
		// fetch transactions to it, after the transactions is fetched, delete it in the pool
		txSend := make([]*core.Transaction, 0)
		firstPtr := 0
		for secondPtr := 0; secondPtr < len(pphm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
			ptx := pphm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
			// if this is a normal transaction or ctx1 before re-sharding && the addr is correspond
			_, ok1 := addrSet[ptx.Sender]
			condition1 := ok1 && !ptx.Relayed
			// if this tx is ctx2
			_, ok2 := addrSet[ptx.Recipient]
			condition2 := ok2 && ptx.Relayed
			if condition1 || condition2 {
				txSend = append(txSend, ptx)
			} else {
				pphm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
				firstPtr++
			}
		}
		pphm.pbftNode.CurChain.Txpool.TxQueue = pphm.pbftNode.CurChain.Txpool.TxQueue[:firstPtr]

		pphm.pbftNode.pl.Plog.Printf("The txSend to shard %d is generated \n", i)
		ast := message.AccountStateAndTx{
			Addrs:        addrSend,
			AccountState: asSend,
			FromShard:    pphm.pbftNode.ShardID,
			Txs:          txSend,
		}
		aByte, err := json.Marshal(ast)
		if err != nil {
			log.Panic()
		}
		send_msg := message.MergeMessage(message.AccountState_and_TX, aByte)
		networks.TcpDial(send_msg, pphm.pbftNode.ip_nodeTable[i][0])
		pphm.pbftNode.pl.Plog.Printf("The message to shard %d is sent\n", i)
	}
	pphm.pbftNode.pl.Plog.Println("after sending, The size of tx pool is: ", len(pphm.pbftNode.CurChain.Txpool.TxQueue))
	pphm.pbftNode.CurChain.Txpool.GetUnlocked()
}

// fetch collect infos
func (pphm *ProposalPbftInsideExtraHandleMod) getCollectOver() bool {
	pphm.cdm.CollectLock.Lock()
	defer pphm.cdm.CollectLock.Unlock()
	return pphm.cdm.CollectOver
}

// propose a partition message
func (pphm *ProposalPbftInsideExtraHandleMod) proposePartition() (bool, *message.Request) {
	pphm.pbftNode.pl.Plog.Printf("S%dN%d : begin partition proposing\n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID)
	// add all data in pool into the set
	for _, at := range pphm.cdm.AccountStateTx {
		for i, addr := range at.Addrs {
			pphm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
		}
		pphm.cdm.ReceivedNewTx = append(pphm.cdm.ReceivedNewTx, at.Txs...)
	}
	// propose, send all txs to other nodes in shard
	pphm.pbftNode.pl.Plog.Println("The number of ReceivedNewTx: ", len(pphm.cdm.ReceivedNewTx))
	for _, tx := range pphm.cdm.ReceivedNewTx {
		if !tx.Relayed && pphm.cdm.ModifiedMap[pphm.cdm.AccountTransferRound][tx.Sender] != pphm.pbftNode.ShardID {
			log.Panic("error tx")
		}
		if tx.Relayed && pphm.cdm.ModifiedMap[pphm.cdm.AccountTransferRound][tx.Recipient] != pphm.pbftNode.ShardID {
			log.Panic("error tx")
		}
	}
	pphm.pbftNode.CurChain.Txpool.AddTxs2Pool(pphm.cdm.ReceivedNewTx)
	pphm.pbftNode.pl.Plog.Println("The size of txpool: ", len(pphm.pbftNode.CurChain.Txpool.TxQueue))

	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range pphm.cdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
	}
	atm := message.AccountTransferMsg{
		ModifiedMap:  pphm.cdm.ModifiedMap[pphm.cdm.AccountTransferRound],
		Addrs:        atmaddr,
		AccountState: atmAs,
		ATid:         uint64(len(pphm.cdm.ModifiedMap)),
	}
	atmbyte := atm.Encode()
	r := &message.Request{
		RequestType: message.PartitionReq,
		Msg: message.RawMessage{
			Content: atmbyte,
		},
		ReqTime: time.Now(),
	}
	return true, r
}

// all nodes in a shard will do accout Transfer, to sync the state trie
func (pphm *ProposalPbftInsideExtraHandleMod) accountTransfer_do(atm *message.AccountTransferMsg) {
	// change the partition Map
	cnt := 0
	for key, val := range atm.ModifiedMap {
		cnt++
		pphm.pbftNode.CurChain.Update_PartitionMap(key, val)
	}
	pphm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", cnt)
	// add the account into the state trie
	pphm.pbftNode.pl.Plog.Printf("%d addrs to add\n", len(atm.Addrs))
	pphm.pbftNode.pl.Plog.Printf("%d accountstates to add\n", len(atm.AccountState))
	pphm.pbftNode.CurChain.AddAccounts(atm.Addrs, atm.AccountState)

	if uint64(len(pphm.cdm.ModifiedMap)) != atm.ATid {
		pphm.cdm.ModifiedMap = append(pphm.cdm.ModifiedMap, atm.ModifiedMap)
	}
	pphm.cdm.AccountTransferRound = atm.ATid
	pphm.cdm.AccountStateTx = make(map[uint64]*message.AccountStateAndTx)
	pphm.cdm.ReceivedNewAccountState = make(map[string]*core.AccountState)
	pphm.cdm.ReceivedNewTx = make([]*core.Transaction, 0)
	pphm.cdm.PartitionOn = false

	pphm.cdm.CollectLock.Lock()
	pphm.cdm.CollectOver = false
	pphm.cdm.CollectLock.Unlock()

	pphm.cdm.P_ReadyLock.Lock()
	pphm.cdm.PartitionReady = make(map[uint64]bool)
	pphm.cdm.P_ReadyLock.Unlock()

	pphm.pbftNode.CurChain.PrintBlockChain()
}
