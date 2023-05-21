package tx

import (
	bm "buffer_manager"
	fm "file_manager"
	lm "log_manager"
)

type RecoveryManager struct {
	logManager    *lm.LogManager
	bufferManager *bm.BufferManager
	tx            *Transaction
	txNum         int32
}

func NewRecoveryManager(logManager *lm.LogManager, bufferManager *bm.BufferManager,
	tx *Transaction, txNum int32) *RecoveryManager {
	rm := &RecoveryManager{
		logManager:    logManager,
		bufferManager: bufferManager,
		tx:            tx,
		txNum:         txNum,
	}
	//产生一条start日志
	WriteStartLog(uint64(txNum), logManager)

	return rm
}

func (r *RecoveryManager) Commit() error {
	err := r.bufferManager.FlushAll(r.txNum)
	if err != nil {
		return err
	}

	lsn, err := WriteCommitLog(uint64(r.txNum), r.logManager)
	if err != nil {
		return err
	}

	return r.logManager.FlushByLSN(lsn)
}

func (r *RecoveryManager) Rollback() error {
	r.doRollback()
	err := r.bufferManager.FlushAll(r.txNum)
	if err != nil {
		return err
	}

	lsn, err := WriteRollbackLog(uint64(r.txNum), r.logManager)
	if err != nil {
		return err
	}

	return r.logManager.FlushByLSN(lsn)
}

func (r *RecoveryManager) Recover() error {
	r.doRecover()
	err := r.bufferManager.FlushAll(r.txNum)
	lsn, err := WriteCheckpointLog(r.logManager)
	if err != nil {
		return err
	}

	err = r.logManager.FlushByLSN(lsn)
	if err != nil {
		return err
	}

	return nil
}

func (r *RecoveryManager) SetInt(buff *bm.Buffer, offset uint64) (uint64, error) {
	oldVal := buff.Contents().GetInt(offset)
	blk := buff.Block()
	return WriteSetIntLog(r.logManager, uint64(r.txNum), blk, offset, oldVal)
}

func (r *RecoveryManager) SetString(buff *bm.Buffer, offset uint64) (uint64, error) {
	oldVal := buff.Contents().GetString(offset)
	blk := buff.Block()
	return WriteSetStringLog(r.logManager, uint64(r.txNum), blk, offset, oldVal)
}

func (r *RecoveryManager) CreateLogRecord(bytes []byte) LogRecordInterface {
	p := fm.NewPageByBytes(bytes)
	switch RecordType(p.GetInt(0)) {
	case START:
		return NewStartRecord(p)
	case COMMIT:
		return NewCommitRecord(p)
	case ROLLBACK:
		return NewRollbackRecord(p)
	case CHECKPOINT:
		return NewCheckpointRecord()
	case SETINT:
		return NewSetIntRecord(p)
	case SETSTRING:
		return NewSetStringRecord(p)
	default:
		panic("unknown log interface")
	}
}

func (r *RecoveryManager) doRollback() {
	iter := r.logManager.Iterator()
	for iter.HasNext() {
		bytes := iter.Next()
		logRecord := r.CreateLogRecord(bytes)
		if logRecord.TxNumber() == uint64(r.txNum) {
			if logRecord.Op() == START {
				return
			}

			logRecord.Undo(r.tx)
		}
	}
}

func (r *RecoveryManager) doRecover() {
	finishedTx := make(map[uint64]bool)
	iter := r.logManager.Iterator()
	for iter.HasNext() {
		bytes := iter.Next()
		logRecord := r.CreateLogRecord(bytes)
		if logRecord.Op() == CHECKPOINT {
			return
		}
		if logRecord.Op() == COMMIT || logRecord.Op() == ROLLBACK {
			finishedTx[logRecord.TxNumber()] = true
		}

		finished, ok := finishedTx[logRecord.TxNumber()]
		if !ok || !finished {
			logRecord.Undo(r.tx)
		}
	}
}
