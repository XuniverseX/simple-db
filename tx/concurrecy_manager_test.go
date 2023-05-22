package tx

import (
	bm "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	"sync"
	"testing"
	"time"
)

func TestCurrencyManager(_ *testing.T) {
	var wg sync.WaitGroup
	fileManager, _ := fm.NewFileManager("txtest", 400)
	logManager, _ := lm.NewLogManager(fileManager, "logfile")
	bufferManager := bm.NewBufferManager(fileManager, logManager, 3)
	//tx.NewTransation(file_manager, log_manager, buffer_manager)
	wg.Add(3)
	go func() {
		defer wg.Done()
		txA := NewTransaction(fileManager, logManager, bufferManager)
		blk1 := fm.NewBlockId("testfile", 1)
		blk2 := fm.NewBlockId("testfile", 2)
		txA.Pin(blk1)
		txA.Pin(blk2)
		fmt.Println("Tx A: request SLock 1")
		txA.GetInt(blk1, 0) //如果返回错误，我们应该放弃执行下面操作并执行回滚，这里为了测试而省略
		fmt.Println("Tx A: receive SLock 1")
		time.Sleep(2 * time.Second)
		fmt.Println("Tx A: request SLock 2")
		txA.GetInt(blk2, 0)
		fmt.Println("Tx A: receive SLock 2")
		fmt.Println("Tx A: Commit")
		txA.Commit()
	}()

	go func() {
		defer wg.Done()
		time.Sleep(1000 * time.Millisecond)
		txB := NewTransaction(fileManager, logManager, bufferManager)
		blk1 := fm.NewBlockId("testfile", 1)
		blk2 := fm.NewBlockId("testfile", 2)
		txB.Pin(blk1)
		txB.Pin(blk2)
		fmt.Println("Tx B: request XLock 2")
		txB.SetInt(blk2, 0, 0, false)
		fmt.Println("Tx B: receive XLock 2")
		time.Sleep(2000 * time.Millisecond)
		fmt.Println("Tx B: request SLock 1")
		txB.GetInt(blk1, 0)
		fmt.Println("Tx B: receive SLock 1")
		fmt.Println("Tx B: Commit")
		txB.Commit()
	}()

	go func() {
		defer wg.Done()
		time.Sleep(2000 * time.Millisecond)
		txC := NewTransaction(fileManager, logManager, bufferManager)
		blk1 := fm.NewBlockId("testfile", 1)
		blk2 := fm.NewBlockId("testfile", 2)
		txC.Pin(blk1)
		txC.Pin(blk2)
		fmt.Println("Tx C: request XLock 1")
		txC.SetInt(blk1, 0, 0, false)
		fmt.Println("Tx C: receive XLock 1")
		time.Sleep(1000 * time.Millisecond)
		fmt.Println("Tx C: request SLock 2")
		txC.GetInt(blk2, 0)
		fmt.Println("Tx C: receive SLock 2")
		fmt.Println("Tx C: Commit")
		txC.Commit()
	}()

	wg.Wait()

	//var d sync.RWMutex
}
