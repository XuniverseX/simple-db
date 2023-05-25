package tx

import (
	fm "file_manager"
)

type ConcurrencyManager struct {
	lockTable *LockTable
	lockMap   map[fm.BlockId]string
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
		c.lockMap[*blk] = "S"
	}

	return nil
}

func (c *ConcurrencyManager) XLock(blk *fm.BlockId) error {
	if !c.hasXLock(blk) {
		c.SLock(blk) //判断区块是否已经被加上共享锁，如果别人已经获得共享锁那么就会挂起
		/*
			之所以在获取写锁之前获取读锁，是因为同一个线程可以在获得读锁的情况下再获取写锁。
			获取读锁时，读锁的计数会加1，如果读锁的计数大于1，说明其他线程对同一个区块加了读锁，
			此时获取写锁就要失败，如果读锁计数只有1，那意味着读锁是上面获取的，也就是同一个线程获取到了读锁
			于是，同一个线程就可以在读锁基础上添加写锁
		*/
		err := c.lockTable.XLock(blk)
		if err != nil {
			return err
		}
		c.lockMap[*blk] = "X"
	}
	return nil
}

func (c *ConcurrencyManager) Release() {
	for key := range c.lockMap {
		c.lockTable.Unlock(&key)
	}
}

func (c *ConcurrencyManager) hasXLock(blk *fm.BlockId) bool {
	lockType, ok := c.lockMap[*blk]
	return ok && lockType == "X"
}
