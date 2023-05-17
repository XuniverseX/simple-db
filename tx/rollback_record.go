package tx

import (
	fm "file_manager"
	"fmt"
	lm "log_manager"
)

type RollbackRecord struct {
	txNum uint64
}

func NewRollbackRecord(p *fm.Page) *RollbackRecord {
	txNum := p.GetInt(UINT64_LENGTH)
	return &RollbackRecord{
		txNum: txNum,
	}
}

func (r *RollbackRecord) Op() RecordType {
	return ROLLBACK
}

func (r *RollbackRecord) TxNumber() uint64 {
	return r.txNum
}

func (r *RollbackRecord) Undo(TransactionInterface) {
	//nothing to do
}

func (r *RollbackRecord) ToString() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.txNum)
}

func WriteRollbackLog(txNum uint64, logManager *lm.LogManager) (uint64, error) {
	record := make([]byte, 2*UINT64_LENGTH)
	p := fm.NewPageByBytes(record)
	p.SetInt(0, uint64(ROLLBACK))
	p.SetInt(UINT64_LENGTH, txNum)

	return logManager.Append(record)
}
