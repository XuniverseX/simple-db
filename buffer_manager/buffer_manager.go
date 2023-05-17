package buffer_manager

import (
	"errors"
	fm "file_manager"
	lm "log_manager"
	"sync"
	"time"
)

const (
	MAX_WAITTIME = 3 //分配缓存最大等待时间
)

type BufferManager struct {
	bufferPool []*Buffer
	available  uint32 //缓存池中有多少可用
	mu         sync.Mutex
}

func NewBufferManager(fileManager *fm.FileManager, logManager *lm.LogManager, bufferSize uint32) *BufferManager {
	bm := &BufferManager{
		available: bufferSize,
	}

	for i := uint32(0); i < bufferSize; i++ {
		buff := NewBuffer(fileManager, logManager)
		bm.bufferPool = append(bm.bufferPool, buff)
	}

	return bm
}

func (bm *BufferManager) Available() uint32 {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	return bm.available
}

// FlushAll 将给定事务号的缓存全部写入磁盘
func (bm *BufferManager) FlushAll(txNum int32) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, buff := range bm.bufferPool {
		if buff.txNum == txNum {
			buff.Flush()
		}
	}
}

// Pin 将给定区块数据分配给缓存页面
func (bm *BufferManager) Pin(blk *fm.BlockId) (*Buffer, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	start := time.Now()
	buff := bm.tryPin(blk) //尝试分配缓存

	for buff == nil && !bm.waitingTooLong(start) {
		//如果无法分配到缓存，等待一段时间再尝试
		time.Sleep(MAX_WAITTIME * time.Second)
		buff = bm.tryPin(blk)
		if buff == nil {
			return nil, errors.New("no buffer available, care for dead lock")
		}
	}

	return buff, nil
}

func (bm *BufferManager) Unpin(buff *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if buff == nil {
		return
	}

	buff.Unpin()
	if !buff.IsPinned() {
		bm.available++
		//notifyAll() //唤醒所有等待线程
	}
}

func (bm *BufferManager) waitingTooLong(start time.Time) bool {
	elapsed := time.Since(start).Seconds()
	if elapsed >= MAX_WAITTIME {
		return true
	}
	return false
}

func (bm *BufferManager) tryPin(blk *fm.BlockId) *Buffer {
	//看看给定区块是否已经读入某个页面
	buff := bm.findExistingBuffer(blk)
	if buff == nil {
		//查看是否还有可用缓存页面
		buff = bm.chooseUnpinBuffer()
		if buff == nil {
			return nil
		}
		buff.AssignToBlock(blk)
	}

	if !buff.IsPinned() {
		bm.available--
	}

	buff.Pin()
	return buff
}

func (bm *BufferManager) findExistingBuffer(blk *fm.BlockId) *Buffer {
	for _, buff := range bm.bufferPool {
		buffBlk := buff.Block()
		if buffBlk != nil && buffBlk.Equal(blk) {
			return buff
		}
	}

	return nil
}

func (bm *BufferManager) chooseUnpinBuffer() *Buffer {
	for _, buff := range bm.bufferPool {
		if !buff.IsPinned() {
			return buff
		}
	}

	return nil
}
