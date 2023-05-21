package tx

import (
	fm "file_manager"
	lm "log_manager"
	"math"
)

type CheckpointRecord struct {
}

func NewCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}

func (c *CheckpointRecord) Op() RecordType {
	return CHECKPOINT
}

func (c *CheckpointRecord) TxNumber() uint64 {
	return math.MaxUint64 //它没有对应的交易号
}

func (c *CheckpointRecord) Undo(tx TransactionInterface) {}

func (c *CheckpointRecord) ToString() string {
	return "<CHECKPOINT>"
}

func WriteCheckpointLog(logManager *lm.LogManager) (uint64, error) {
	rec := make([]byte, UINT64_LENGTH)
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(CHECKPOINT))

	return logManager.Append(rec)
}
