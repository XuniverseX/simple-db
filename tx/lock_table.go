package tx

import (
	"errors"
	fm "file_manager"
	"fmt"
	"sync"
	"time"
)

const (
	MAX_WAITING_TIME = 10 //3用于测试，正式使用时设置为10
)

type LockTable struct {
	lockMap    map[fm.BlockId]int64           //将锁与区块对应，互斥锁为-1，共享锁 > 0
	notifyChan map[fm.BlockId]chan struct{}   //用于通知挂起的所有协程恢复执行的管道
	notifyWg   map[fm.BlockId]*sync.WaitGroup //实现唤醒通知
	mu         sync.Mutex
}

var (
	lockTableInstance *LockTable
	once              sync.Once
)

func getLockTableInstance() *LockTable {
	once.Do(func() {
		if lockTableInstance == nil {
			lockTableInstance = newLockTable()
		}
	})

	return lockTableInstance
}

func newLockTable() *LockTable {
	return &LockTable{
		lockMap:    make(map[fm.BlockId]int64),
		notifyChan: make(map[fm.BlockId]chan struct{}),
		notifyWg:   make(map[fm.BlockId]*sync.WaitGroup),
	}
}

func (l *LockTable) initWaitingBlock(blk *fm.BlockId) {
	_, ok := l.notifyChan[*blk]
	if !ok {
		l.notifyChan[*blk] = make(chan struct{})
	}

	_, ok = l.notifyWg[*blk]
	if !ok {
		l.notifyWg[*blk] = &sync.WaitGroup{}
	}
}

func (l *LockTable) waitingUntilTimeoutOrNotified(blk *fm.BlockId) {
	//wg, ok := l.notifyWg[*blk]
	//if !ok {
	//	var newWg sync.WaitGroup
	//	l.notifyWg[*blk] = &newWg
	//	wg = &newWg
	//}
	//wg.Add(1)
	//defer wg.Done()
	l.mu.Unlock() //挂起之前释放方法锁
	select {
	case <-time.After(MAX_WAITING_TIME * time.Second):
		fmt.Printf("%v routine wake up, timeout\n", *blk)
	case <-l.notifyChan[*blk]:
		fmt.Printf("%v routine wake up, notify\n", *blk)
	}
	l.mu.Lock() //抢占锁
}

func (l *LockTable) notifyAll(blk *fm.BlockId) {
	fmt.Printf("close channel for blk: %v\n", *blk)

	channel, ok := l.notifyChan[*blk]
	if !ok {
		fmt.Printf("channel of %v is already closed\n", *blk)
		return
	}

	close(channel)
	delete(l.notifyChan, *blk)

	time.Sleep(5 * time.Millisecond) //确保等待的协程都取消等待

	l.notifyChan[*blk] = make(chan struct{})

	//go func(blkUnlock fm.BlockId) {
	//	//等待所有线程返回后再重新设置channel
	//	//注意这个线程不一定得到及时调度，因此可能不能及时创建channel对象从而导致close closed channel panic
	//	l.notifyWg[blkUnlock].Wait()
	//	l.mu.Lock() //访问内部数据时需要加锁
	//	l.notifyChan[blkUnlock] = make(chan struct{})
	//	l.mu.Unlock()
	//}(*blk)
	//l.notifyWg[*blk].Wait()
}

func (l *LockTable) SLock(blk *fm.BlockId) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.initWaitingBlock(blk)

	start := time.Now()
	for l.hasXLock(blk) && !l.waitingTooLong(start) {
		fmt.Println("get SLock fail and sleep")
		l.waitingUntilTimeoutOrNotified(blk) //对应协程挂起给定的时间
	}

	if l.hasXLock(blk) {
		fmt.Println("SLock failed because XLock on given blk")
		return errors.New("error: XLock on given blk")
	}

	//defer l.mu.Unlock()
	val := l.getLockVal(blk)
	l.lockMap[*blk] = val + 1
	return nil
}

func (l *LockTable) XLock(blk *fm.BlockId) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.initWaitingBlock(blk)

	start := time.Now()
	for l.hasOtherSLock(blk) && !l.waitingTooLong(start) {
		fmt.Println("get XLock fail and sleep")
		l.waitingUntilTimeoutOrNotified(blk)
	}

	if l.hasOtherSLock(blk) {
		fmt.Println("XLock failed because SLock on given blk")
		return errors.New("error: SLock on given blk")
	}

	l.lockMap[*blk] = -1 //-1表示区块被加上互斥锁
	return nil
}

func (l *LockTable) Unlock(blk *fm.BlockId) {
	l.mu.Lock()
	defer l.mu.Unlock()

	val := l.getLockVal(blk)
	if val > 0 {
		l.lockMap[*blk] = val - 1
		l.notifyAll(blk) //释放读锁也要notify
	} else {
		//通知所有等待给定区块的线程从Wait中恢复
		fmt.Printf("unlock by blk: + %v\n", *blk)
		l.lockMap[*blk] = 0
		l.notifyAll(blk)
	}
}

func (l *LockTable) getLockVal(blk *fm.BlockId) int64 {
	val, ok := l.lockMap[*blk]
	if !ok {
		l.lockMap[*blk] = 0
		return 0
	}

	return val
}

func (l *LockTable) hasXLock(blk *fm.BlockId) bool {
	return l.getLockVal(blk) == -1
}

func (l *LockTable) hasOtherSLock(blk *fm.BlockId) bool {
	return l.getLockVal(blk) > 0
}

func (l *LockTable) waitingTooLong(start time.Time) bool {
	elapsed := time.Since(start).Seconds()
	if elapsed >= MAX_WAITING_TIME {
		return true
	}

	return false
}
