package tx

import (
	fm "file_manager"
	"fmt"
	lg "log_manager"
)

type SetIntRecord struct {
	txNum  uint64
	offset uint64
	val    uint64
	blk    *fm.BlockId
}

func NewSetIntRecord(p *fm.Page) *SetIntRecord {
	tPos := uint64(UINT64_LENGTH)
	txNum := p.GetInt(tPos)
	fPos := tPos + UINT64_LENGTH
	filename := p.GetString(fPos)
	bPos := fPos + fm.MaxLengthOfStringInPage(filename)
	blkNum := p.GetInt(bPos)
	blk := fm.NewBlockId(filename, blkNum)
	opos := bPos + UINT64_LENGTH
	offset := p.GetInt(opos)
	vPos := opos + UINT64_LENGTH
	val := p.GetInt(vPos) //将日志中的字符串再次写入给定位置

	return &SetIntRecord{
		txNum:  txNum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func (s *SetIntRecord) Op() RecordType {
	return SETINT
}

func (s *SetIntRecord) TxNumber() uint64 {
	return s.txNum
}

func (s *SetIntRecord) ToString() string {
	return fmt.Sprintf("<SETINT %d %d %d %d>", s.txNum, s.blk.Number(), s.offset, s.val)
}

func (s *SetIntRecord) Undo(tx TransactionInterface) {
	tx.Pin(s.blk)
	tx.SetInt(s.blk, s.offset, int64(s.val), false) //将原来的数字写回去
	tx.Unpin(s.blk)
}

func WriteSetIntLog(logManager *lg.LogManager, txNum uint64,
	blk *fm.BlockId, offset uint64, val uint64) (uint64, error) {

	tPos := uint64(UINT64_LENGTH)
	fPos := tPos + UINT64_LENGTH
	bPos := fPos + fm.MaxLengthOfStringInPage(blk.FileName())
	opos := bPos + UINT64_LENGTH
	vPos := opos + UINT64_LENGTH
	recLen := vPos + UINT64_LENGTH
	rec := make([]byte, recLen)

	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(SETINT))
	p.SetInt(tPos, txNum)
	p.SetString(fPos, blk.FileName())
	p.SetInt(bPos, blk.Number())
	p.SetInt(opos, offset)
	p.SetInt(vPos, val)

	return logManager.Append(rec)
}
