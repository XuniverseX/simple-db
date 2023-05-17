package tx

import (
	fm "file_manager"
)

type TransactionInterface interface {
	Commit()
	Rollback()
	Recover()
	Pin(blk *fm.BlockId)
	Unpin(blk *fm.BlockId)
	GetInt(blk *fm.BlockId, offset uint64) uint64
	GetString(blk *fm.BlockId, offset uint64) string
	SetInt(blk *fm.BlockId, offset uint64, val uint64, okToLog bool)
	SetString(blk *fm.BlockId, offset uint64, val string, okToLog bool)
	Available() uint64
	Size(filename string) uint64
	Append(filename string) *fm.BlockId
	BlockSize() uint64
}

type RecordType uint64

const (
	CHECKPOINT RecordType = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

const (
	UINT64_LENGTH = 8
)

type LogRecordInterface interface {
	Op() RecordType               //返回记录的类别
	TxNumber() uint64             //对应交易的号码
	Undo(tx TransactionInterface) //执行日志操作
	ToString() string             //获得记录的字符串内容
}
