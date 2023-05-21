package main

import (
	bm "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	"tx"
)

func main() {
	fileManager, _ := fm.NewFileManager("txtest", 400)
	logManager, _ := lm.NewLogManager(fileManager, "logfile")
	bufferManager := bm.NewBufferManager(fileManager, logManager, 3)

	tx1 := tx.NewTransaction(fileManager, logManager, bufferManager)
	blk := fm.NewBlockId("testfile", 1)
	tx1.Pin(blk)
	//设置log为false，因为一开始数据没有任何意义，因此不能进行日志记录
	tx1.SetInt(blk, 80, 1, false)
	tx1.SetString(blk, 40, "one", false)
	tx1.Commit() //回滚后，数据会到这里

	tx2 := tx.NewTransaction(fileManager, logManager, bufferManager)
	tx2.Pin(blk)
	IVal, _ := tx2.GetInt(blk, 80)
	SVal, _ := tx2.GetString(blk, 40)
	fmt.Println("initial value at location 80 =", IVal)
	fmt.Println("initial value at location 40 =", SVal)
	newIVal := IVal + 1
	newSVal := SVal + "!"
	tx2.SetInt(blk, 80, newIVal, true)
	tx2.SetString(blk, 40, newSVal, true)
	tx2.Commit()

	tx3 := tx.NewTransaction(fileManager, logManager, bufferManager)
	tx3.Pin(blk)
	IVal, _ = tx3.GetInt(blk, 80)
	SVal, _ = tx3.GetString(blk, 40)
	fmt.Println("new value at location 80 =", IVal)
	fmt.Println("new value at location 40 =", SVal)
	tx3.SetInt(blk, 80, 999, true)
	IVal, _ = tx3.GetInt(blk, 80)
	fmt.Println("before rollback, value at location 80 =", IVal)
	tx3.Rollback()

	tx4 := tx.NewTransaction(fileManager, logManager, bufferManager)
	tx4.Pin(blk)
	IVal, _ = tx4.GetInt(blk, 80)
	fmt.Println("after rollback at location 80 =", IVal)
	tx4.Commit()
}
