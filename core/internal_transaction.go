package core

import (
	"blockEmulator/utils"
	"fmt"
	"math/big"
	"time"
)

type InternalTransaction struct {
	Sender       utils.Address
	Recipient    utils.Address
	Value        *big.Int
	ParentTxHash string

	Time time.Time // TimeStamp the tx proposed.

	// used in transaction relaying
	Relayed bool

	SenderIsContract    bool
	RecipientIsContract bool
}

func (itx *InternalTransaction) PrintTx() string {
	vals := []interface{}{
		itx.Sender[:],
		itx.Recipient[:],
		itx.Value,
		string(itx.ParentTxHash[:]),
	}
	res := fmt.Sprintf("%v\n", vals)
	return res
}

// new a internal transaction
func NewInternalTransaction(sender, recipient, parentTxHash string, value *big.Int, nonce uint64, proposeTime time.Time, senderIsContract, recipientIsContract bool) *InternalTransaction {
	itx := &InternalTransaction{
		Sender:              sender,
		Recipient:           recipient,
		Value:               value,
		Time:                proposeTime,
		SenderIsContract:    senderIsContract,
		RecipientIsContract: recipientIsContract,
	}

	return itx
}
