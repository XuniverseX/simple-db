package log_manager

import (
	fm "file_manager"
)

// LogIterator用于遍历区块内的日志，日志从底部王往上写，遍历自上而下读取
type LogIterator struct {
	fileManager *fm.FileManager
	blk         *fm.BlockId
	p           *fm.Page
	currentPos  uint64 //当前遍历的偏移
	boundary    uint64
}

func NewLogIterator(fileManager *fm.FileManager, blk *fm.BlockId) *LogIterator {
	it := LogIterator{
		fileManager: fileManager,
		blk:         blk,
	}

	it.p = fm.NewPageBySize(fileManager.BlockSize())
	err := it.moveToMem(blk)
	if err != nil {
		return nil
	}

	return &it
}

// 将对应区块的数据从磁盘读入内存
func (l *LogIterator) moveToMem(blk *fm.BlockId) error {
	_, err := l.fileManager.Read(blk, l.p)
	if err != nil {
		return err
	}

	//获得日志的起始地址
	l.boundary = l.p.GetInt(0)
	l.currentPos = l.boundary

	return nil
}

func (l *LogIterator) Next() []byte {
	//先读取最新日志，也就是编号大的
	if l.currentPos == l.fileManager.BlockSize() {
		l.blk = fm.NewBlockId(l.blk.FileName(), l.blk.Number()-1)
		l.moveToMem(l.blk)
	}

	record := l.p.GetBytes(l.currentPos)
	l.currentPos += UINT64_LEN + uint64(len(record))

	return record
}

func (l *LogIterator) HasNext() bool {
	//如果当前偏移位置小于区块大小，那么还有数据可以从当前区块读取
	//如果当前区块数据已经全部读完，但是区块号不为0，那么可以读取前面区块获得老的日志数据
	return l.currentPos < l.fileManager.BlockSize() || l.blk.Number() > 0
}
