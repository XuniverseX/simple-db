package tx

import (
	fm "file_manager"
	"fmt"
	lm "log_manager"
)

/*
<SETSTRING, 0, junk, 33, 12, joe, joseph> 7个字段
可以用6个字段实现，将原数据先SETSTRING一遍即可（注意日志是从下往上写）
...
<SETSTRING, 0, junk, 33, 12, joe>
<SETSTRING, 0, junk, 33, 12, joseph>
...
在回滚的时候，我们从上往下读取，因此我们会先读到joe,然后读到joseph，于是执行回滚时我们只要把
读到的字符串写入到给定位置就可以，例如我们先读到joe，然后写入junk文件区块为33偏移为12的地方，
然后又读取joseph，再次将它写入到junk文件区块为33偏移为12的地方，于是就实现了回滚效果，
*/

type SetStringRecord struct {
	txNum  uint64
	offset uint64
	val    string
	blk    *fm.BlockId
}

func NewSetStringRecord(p *fm.Page) *SetStringRecord {
	//<SETSTRING 事务号 文件名 区块号 偏移量 value>
	//头8字节为日志类型
	tPos := uint64(UINT64_LENGTH)
	txNum := p.GetInt(tPos)
	fPos := tPos + UINT64_LENGTH
	filename := p.GetString(fPos)
	bPos := fPos + p.MaxLengthOfString(filename)
	blkNum := p.GetInt(bPos)
	blk := fm.NewBlockId(filename, blkNum)
	oPos := bPos + UINT64_LENGTH
	offset := p.GetInt(oPos)
	vPos := oPos + UINT64_LENGTH
	val := p.GetString(vPos) //将日志中的字符串再次写入给定位置

	return &SetStringRecord{
		txNum:  txNum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func (s *SetStringRecord) Op() RecordType {
	return SETSTRING
}

func (s *SetStringRecord) TxNumber() uint64 {
	return s.txNum
}

func (s *SetStringRecord) ToString() string {
	return fmt.Sprintf("<SETSTRING %d %d %d %s>", s.txNum, s.blk.Number(), s.offset, s.val)
}

func (s *SetStringRecord) Undo(tx TransactionInterface) {
	tx.Pin(s.blk)
	tx.SetString(s.blk, s.offset, s.val, false) //将原来的字符串写回去
	tx.Unpin(s.blk)
}

// WriteSetStringLog 构造字符串内容的日志,SetStringRecord在构造中默认给定缓存页面已经有了字符串信息,
// 但是在初始状态，缓存页面可能还没有相应日志信息，这个接口的作用就是为给定缓存写入字符串日志
func WriteSetStringLog(logManager *lm.LogManager, txNum uint64,
	blk *fm.BlockId, offset uint64, val string) (uint64, error) {
	tPos := uint64(UINT64_LENGTH)
	fPos := tPos + UINT64_LENGTH
	p := fm.NewPageBySize(1) //用于调用MaxLengthOfString()
	bPos := fPos + p.MaxLengthOfString(blk.FileName())
	oPos := bPos + UINT64_LENGTH
	vPos := oPos + UINT64_LENGTH
	recLen := vPos + p.MaxLengthOfString(val)
	rec := make([]byte, recLen)

	p = fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(SETSTRING))
	p.SetInt(tPos, txNum)
	p.SetString(fPos, blk.FileName())
	p.SetInt(bPos, blk.Number())
	p.SetInt(oPos, offset)
	p.SetString(vPos, val)

	return logManager.Append(rec)
}
