package tx

import (
	fm "file_manager"
	"fmt"
	lm "log_manager"
)

type CommitRecord struct {
	txNum uint64
}

func NewCommitRecord(p *fm.Page) *CommitRecord {
	txNum := p.GetInt(UINT64_LENGTH)
	return &CommitRecord{
		txNum: txNum,
	}
}

func (c *CommitRecord) Op() RecordType {
	return COMMIT
}

func (c *CommitRecord) TxNumber() uint64 {
	return c.txNum
}

func (c *CommitRecord) Undo(TransactionInterface) {
	//nothing to do
}

func (c *CommitRecord) ToString() string {
	return fmt.Sprintf("<COMMIT %d>", c.txNum)
}

func WriteCommitLog(txNum uint64, logManager *lm.LogManager) (uint64, error) {
	record := make([]byte, 2*UINT64_LENGTH)
	p := fm.NewPageByBytes(record)
	p.SetInt(0, uint64(COMMIT))
	p.SetInt(UINT64_LENGTH, txNum)

	return logManager.Append(record)
}
