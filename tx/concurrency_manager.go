package tx

import (
	fm "file_manager"
	"sync"
)

type ConcurrencyManager struct {
	lockTable *LockTable
	lockMap   map[fm.BlockId]string
	mu        sync.Mutex
}

func NewConcurrencyManager() *ConcurrencyManager {
	return &ConcurrencyManager{
		lockTable: getLockTableInstance(),
		lockMap:   make(map[fm.BlockId]string),
	}
}

func (c *ConcurrencyManager) SLock(blk *fm.BlockId) error {
	_, ok := c.lockMap[*blk]
	if !ok {
		err := c.lockTable.SLock(blk)
		if err != nil {
			return err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.lockMap[*blk] = "S"
	}

	return nil
}

func (c *ConcurrencyManager) XLock(blk *fm.BlockId) error {
	if !c.hasXLock(blk) {
		//c.SLock(blk) //判断区块是否已经被加上共享锁，如果别人已经获得共享锁那么就会挂起
		err := c.lockTable.XLock(blk)
		if err != nil {
			return err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.lockMap[*blk] = "X"
	}
	return nil
}

func (c *ConcurrencyManager) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.lockMap {
		c.lockTable.Unlock(&key)
	}
}

func (c *ConcurrencyManager) hasXLock(blk *fm.BlockId) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	lockType, ok := c.lockMap[*blk]
	return ok && lockType == "X"
}
