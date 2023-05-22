package tx

import (
	"errors"
	fm "file_manager"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestRoutinesWithSLockTimeout(t *testing.T) {
	var errArray []error
	var wg sync.WaitGroup
	blk := fm.NewBlockId("testfile", 1)
	lockTable := getLockTableInstance()
	lockTable.XLock(blk)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := lockTable.SLock(blk)
			if err == nil {
				fmt.Println("access SLock ok")
			}
			errArray = append(errArray, err)
		}()
	}
	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	fmt.Println(elapsed)
	require.Equal(t, elapsed >= 3, true)
	require.Equal(t, len(errArray), 3)
	for i := 0; i < 3; i++ {
		require.Equal(t, errArray[i], errors.New("error: XLock on given blk"))
	}
}

func TestRoutinesWithSLockAfterXLockRelease(t *testing.T) {
	var errArray []error
	var wg sync.WaitGroup
	blk := fm.NewBlockId("testfile", 1)
	lockTable := getLockTableInstance()
	lockTable.XLock(blk)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := lockTable.SLock(blk)
			if err == nil {
				fmt.Println("access SLock ok")
			}
			errArray = append(errArray, err)
		}()
	}
	start := time.Now()
	time.Sleep(1 * time.Second) //让线程都运行起来
	lockTable.Unlock(blk)       //释放加在区块上的互斥锁
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	require.Equal(t, elapsed < 3, true)
	require.Equal(t, len(errArray), 3)
	for i := 0; i < 3; i++ {
		require.Nil(t, errArray[i]) //所有线程能获得共享锁然后读取数据
	}

	require.Equal(t, lockTable.lockMap[*blk], int64(3))
}
