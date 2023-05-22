package tx

import (
	bm "buffer_manager"
	"errors"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	"sync"
)

var (
	txNumMu  sync.Mutex
	allTxNum = int32(0)
)

func nextTxNum() int32 {
	txNumMu.Lock()
	defer txNumMu.Unlock()

	allTxNum++

	return allTxNum
}

type Transaction struct {
	concurManager   *ConcurrencyManager
	recoveryManager *RecoveryManager
	fileManager     *fm.FileManager
	logManager      *lm.LogManager
	bufferManager   *bm.BufferManager
	bufferList      *BufferList
	txNum           int32
}

func NewTransaction(fMgr *fm.FileManager, lMgr *lm.LogManager, bMgr *bm.BufferManager) *Transaction {
	txNum := nextTxNum()
	tx := &Transaction{
		fileManager:   fMgr,
		logManager:    lMgr,
		bufferManager: bMgr,
		bufferList:    NewBufferList(bMgr),
		txNum:         txNum,
	}

	//创建同步管理器
	tx.concurManager = NewConcurrencyManager()

	//创建恢复管理器
	tx.recoveryManager = NewRecoveryManager(lMgr, bMgr, tx, txNum)

	return tx
}

func (t *Transaction) Commit() error {
	//释放同步管理器
	t.concurManager.Release()
	//调用恢复管理器执行commit
	err := t.recoveryManager.Commit()
	if err != nil {
		return err
	}

	r := fmt.Sprintf("transaction %d committed", t.txNum)
	fmt.Println(r)

	t.bufferList.UnpinAll()
	return nil
}

func (t *Transaction) Rollback() error {
	//释放同步管理器
	t.concurManager.Release()
	//调用恢复管理器rollback
	err := t.recoveryManager.Rollback()
	if err != nil {
		return err
	}
	r := fmt.Sprintf("transation %d roll back", t.txNum)
	fmt.Println(r)

	t.bufferList.UnpinAll()
	return nil
}

func (t *Transaction) Recover() error {
	//系统启动时会在所有事务执行前执行Recover()
	err := t.bufferManager.FlushAll(t.txNum)
	if err != nil {
		return err
	}

	//调用恢复管理器的recover接口
	return t.recoveryManager.Recover()
}

func (t *Transaction) Pin(blk *fm.BlockId) error {
	return t.bufferList.Pin(blk)
}

func (t *Transaction) Unpin(blk *fm.BlockId) {
	t.bufferList.Unpin(blk)
}

func (t *Transaction) bufferNotExistError(blk *fm.BlockId) error {
	errStr := fmt.Sprintf("No buffer found for given blk : %d with file name: %s\n",
		blk.Number(), blk.FileName())
	return errors.New(errStr)
}

func (t *Transaction) GetInt(blk *fm.BlockId, offset uint64) (int64, error) {
	err := t.concurManager.SLock(blk)
	if err != nil {
		return -1, err
	}
	buff := t.bufferList.getBuffer(blk)
	if buff == nil {
		return -1, t.bufferNotExistError(blk)
	}

	return int64(buff.Contents().GetInt(offset)), nil
}

func (t *Transaction) SetInt(blk *fm.BlockId, offset uint64, val int64, okToLog bool) error {
	//调用同步管理器加x锁
	err := t.concurManager.XLock(blk)
	if err != nil {
		return err
	}

	buff := t.bufferList.getBuffer(blk)
	if buff == nil {
		return t.bufferNotExistError(blk)
	}

	var lsn uint64

	if okToLog {
		lsn, err = t.recoveryManager.SetInt(buff, offset)
		if err != nil {
			return err
		}
	}

	p := buff.Contents()
	p.SetInt(offset, uint64(val))
	buff.SetModified(t.txNum, lsn)

	return nil
}

func (t *Transaction) GetString(blk *fm.BlockId, offset uint64) (string, error) {
	//调用同步管理器加s锁
	err := t.concurManager.SLock(blk)
	if err != nil {
		return "", err
	}

	buff := t.bufferList.getBuffer(blk)
	if buff == nil {
		return "", t.bufferNotExistError(blk)
	}

	return buff.Contents().GetString(offset), nil
}

func (t *Transaction) SetString(blk *fm.BlockId, offset uint64, val string, okToLog bool) error {
	//使用同步管理器加x锁
	err := t.concurManager.XLock(blk)
	if err != nil {
		return err
	}

	buff := t.bufferList.getBuffer(blk)
	if buff == nil {
		return t.bufferNotExistError(blk)
	}

	var lsn uint64

	if okToLog {
		//调用恢复管理器SetString方法
		lsn, err = t.recoveryManager.SetString(buff, offset)
		if err != nil {
			return err
		}
	}

	p := buff.Contents()
	p.SetString(offset, val)
	buff.SetModified(t.txNum, lsn)

	return nil
}

func (t *Transaction) AvailableBuffers() uint64 {
	return uint64(t.bufferManager.Available())
}

func (t *Transaction) Size(filename string) (uint64, error) {
	//调用同步管理器加S锁
	dummyBlk := fm.NewBlockId(filename, uint64(END_OF_FILE))
	err := t.concurManager.SLock(dummyBlk)
	if err != nil {
		return 0, err
	}

	size, err := t.fileManager.Size(filename)
	if err != nil {
		return 0, err
	}

	return size, nil
}

func (t *Transaction) Append(filename string) (*fm.BlockId, error) {
	//调用同步管理器加X锁
	dummyBlk := fm.NewBlockId(filename, END_OF_FILE)
	err := t.concurManager.XLock(dummyBlk)
	if err != nil {
		return nil, err
	}

	blk, err := t.fileManager.Append(filename)
	if err != nil {
		return nil, err
	}

	return &blk, nil
}

func (t *Transaction) BlockSize() uint64 {
	return t.fileManager.BlockSize()
}
