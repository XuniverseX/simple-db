package tx

import (
	fm "file_manager"
	"fmt"
	lm "log_manager"
)

type StartRecord struct {
	txNum uint64
}

func NewStartRecord(p *fm.Page) *StartRecord {
	//开头8字节对应日志类型，后8字节对应事务号
	txNum := p.GetInt(UINT64_LENGTH)
	return &StartRecord{
		txNum: txNum,
	}
}

func (s *StartRecord) Op() RecordType {
	return START
}

func (s *StartRecord) TxNumber() uint64 {
	return s.txNum
}

func (s *StartRecord) Undo(TransactionInterface) {
	//nothing to do
}

func (s *StartRecord) ToString() string {
	return fmt.Sprintf("<START %d>", s.txNum)
}

func WriteStartLog(txNum uint64, logManager *lm.LogManager) (uint64, error) {
	record := make([]byte, 2*UINT64_LENGTH)
	p := fm.NewPageByBytes(record)
	p.SetInt(0, uint64(START))
	p.SetInt(UINT64_LENGTH, txNum)

	return logManager.Append(record)
}
