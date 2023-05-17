package tx

import (
	"encoding/binary"
	fm "file_manager"
	"fmt"
	"github.com/stretchr/testify/require"
	lm "log_manager"
	"testing"
)

const (
	DIR                    = "record_test"
	LOG_FILENAME1          = "record_file"
	LOG_FILENAME_SETSTRING = "setstring"
	LOG_FILENAME_SETINT    = "setint"
)

func TestStartRecord(t *testing.T) {
	fileManager, _ := fm.NewFileManager(DIR, 400)
	logManager, _ := lm.NewLogManager(fileManager, LOG_FILENAME1)

	//fmt.Println(*logManager)
	//size, _ := fileManager.Size(LOG_FILENAME)
	//fmt.Println(size)

	txNum := uint64(13) //交易号
	p := fm.NewPageBySize(32)
	p.SetInt(0, uint64(START))
	p.SetInt(8, txNum)
	startRecord := NewStartRecord(p)
	expectedStr := fmt.Sprintf("<START %d>", txNum)
	require.Equal(t, expectedStr, startRecord.ToString())

	_, err := WriteStartLog(startRecord.TxNumber(), logManager)
	require.Nil(t, err)

	//fmt.Println(*logManager)

	//检查写入的日志是否符号预期
	iter := logManager.Iterator()
	rec := iter.Next()
	recOp := binary.LittleEndian.Uint64(rec[0:8])
	recTxNum := binary.LittleEndian.Uint64(rec[8:])
	require.Equal(t, recOp, uint64(START))
	require.Equal(t, recTxNum, txNum)
}

func TestSetStringRecord(t *testing.T) {
	fileManager, _ := fm.NewFileManager(DIR, 400)
	logManager, _ := lm.NewLogManager(fileManager, LOG_FILENAME_SETSTRING)

	str := "original string"
	blkNum := uint64(1)
	dummyBlk := fm.NewBlockId("dummy_id", blkNum)
	txNum := uint64(1)
	offset := uint64(13)
	//写入用于恢复的日志
	WriteSetStringLog(logManager, txNum, dummyBlk, offset, str)
	iter := logManager.Iterator()
	rec := iter.Next()
	logP := fm.NewPageByBytes(rec)
	setStrRec := NewSetStringRecord(logP)
	expectedStr := fmt.Sprintf("<SETSTRING %d %d %d %s>", txNum, blkNum, offset, str)

	require.Equal(t, expectedStr, setStrRec.ToString())

	newP := fm.NewPageBySize(400)
	newP.SetString(offset, str)
	newP.SetString(offset, "modify string 1")
	newP.SetString(offset, "modify string 2")
	txSub := NewSub(newP)
	setStrRec.Undo(txSub)
	recoverStr := newP.GetString(offset)

	require.Equal(t, recoverStr, str)
}

func TestSetIntRecord(t *testing.T) {
	fileManager, _ := fm.NewFileManager(DIR, 400)
	logManager, _ := lm.NewLogManager(fileManager, LOG_FILENAME_SETINT)

	val := uint64(255)
	blk := uint64(1)
	dummyBlk := fm.NewBlockId("dummy_id", blk)
	txNum := uint64(1)
	offset := uint64(13)
	//写入用于恢复的日志
	WriteSetIntLog(logManager, txNum, dummyBlk, offset, val)
	iter := logManager.Iterator()
	rec := iter.Next()
	logP := fm.NewPageByBytes(rec)
	setIntRec := NewSetIntRecord(logP)
	expectedStr := fmt.Sprintf("<SETINT %d %d %d %d>", txNum, blk, offset, val)

	require.Equal(t, expectedStr, setIntRec.ToString())

	newP := fm.NewPageBySize(400)
	newP.SetInt(offset, val)
	newP.SetInt(offset, 22)
	newP.SetInt(offset, 33)
	txStub := NewSub(newP)
	setIntRec.Undo(txStub)
	recoverVal := newP.GetInt(offset)

	require.Equal(t, recoverVal, val)
}

func TestRollBackRecord(t *testing.T) {
	fileManager, _ := fm.NewFileManager(DIR, 400)
	logManager, _ := lm.NewLogManager(fileManager, "rollback")
	txNum := uint64(255)
	WriteRollbackLog(txNum, logManager)
	iter := logManager.Iterator()
	rec := iter.Next()
	newP := fm.NewPageByBytes(rec)

	rollBackRec := NewRollbackRecord(newP)
	expectedStr := fmt.Sprintf("<ROLLBACK %d>", txNum)

	require.Equal(t, expectedStr, rollBackRec.ToString())
}

func TestCommitRecord(t *testing.T) {
	fileManager, _ := fm.NewFileManager(DIR, 400)
	logManager, _ := lm.NewLogManager(fileManager, "commit")
	txNum := uint64(255)
	WriteCommitLog(txNum, logManager)
	iter := logManager.Iterator()
	rec := iter.Next()
	newP := fm.NewPageByBytes(rec)

	rollBackRec := NewCommitRecord(newP)
	expectedStr := fmt.Sprintf("<COMMIT %d>", txNum)

	require.Equal(t, expectedStr, rollBackRec.ToString())
}

func TestCheckPointRecord(t *testing.T) {
	fileManager, _ := fm.NewFileManager(DIR, 400)
	logManager, _ := lm.NewLogManager(fileManager, "checkpoint")
	WriteCheckpointLog(logManager)
	iter := logManager.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)
	val := pp.GetInt(0)

	require.Equal(t, val, uint64(CHECKPOINT))

	checkPointRec := NewCheckpointRecord()
	expectedStr := "<CHECKPOINT>"
	require.Equal(t, expectedStr, checkPointRec.ToString())
}
