package tx

import (
	fm "file_manager"
	"math"
)

type TransactionInterface interface {
	Commit() error
	Rollback() error
	Recover() error
	Pin(blk *fm.BlockId) error
	Unpin(blk *fm.BlockId)
	GetInt(blk *fm.BlockId, offset uint64) (int64, error)
	SetInt(blk *fm.BlockId, offset uint64, val int64, okToLog bool) error
	GetString(blk *fm.BlockId, offset uint64) (string, error)
	SetString(blk *fm.BlockId, offset uint64, val string, okToLog bool) error
	AvailableBuffers() uint64
	Size(filename string) (uint64, error)
	Append(filename string) (*fm.BlockId, error)
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
	END_OF_FILE   = math.MaxUint64
)

type LogRecordInterface interface {
	Op() RecordType               //返回记录的类别
	TxNumber() uint64             //对应交易的号码
	Undo(tx TransactionInterface) //执行日志操作
	ToString() string             //获得记录的字符串内容
}
