package buffer_manager

import (
	fm "file_manager"
	lm "log_manager"
)

type Buffer struct {
	fileManager *fm.FileManager
	logManager  *lm.LogManager
	contents    *fm.Page //用于存储磁盘数据的缓存页面
	blk         *fm.BlockId
	pins        uint32 //被引用计数
	txNum       int32  //事务号，-1表明没有被修改
	lsn         uint64 //日志号，暂时忽略其作用
}

func NewBuffer(fileManager *fm.FileManager, logManager *lm.LogManager) *Buffer {
	return &Buffer{
		fileManager: fileManager,
		logManager:  logManager,
		txNum:       -1,
		contents:    fm.NewPageBySize(fileManager.BlockSize()),
	}
}

func (b *Buffer) Contents() *fm.Page {
	return b.contents
}

func (b *Buffer) Block() *fm.BlockId {
	return b.blk
}

func (b *Buffer) SetModified(txNum int32, lsn uint64) {
	//如果客户修改了页面数据，必须调用该接口通知Buffer
	b.txNum = txNum
	if lsn > 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) ModifyingTx() int32 {
	return b.txNum
}

// AssignToBlock 将当前缓存用于读取指定磁盘文件的区块数据
func (b *Buffer) AssignToBlock(blk *fm.BlockId) {
	b.Flush() //页面读取其他区块数据时，先将当前缓存数据写入磁盘
	b.blk = blk
	b.fileManager.Read(blk, b.Contents()) //将对应磁盘区块数据读入缓存中
	b.pins = 0
}

func (b *Buffer) Flush() {
	if b.txNum < 0 {
		return
	}

	b.logManager.FlushByLSN(b.lsn)           //为以后的系统崩溃提供支持
	b.fileManager.Write(b.blk, b.Contents()) //将已经修改的数据写回磁盘
	b.txNum = -1
}

// Pin 增加缓存页面引用计数
func (b *Buffer) Pin() {
	b.pins++
}

func (b *Buffer) Unpin() {
	b.pins--
}
