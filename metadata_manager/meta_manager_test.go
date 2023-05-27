package metadata_manager

import (
	bm "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	"math/rand"
	rm "record_manager"
	"testing"
	"tx"
)

func TestMetaDataManager(t *testing.T) {
	fileManager, _ := fm.NewFileManager("recordtest", 400)
	logManager, _ := lm.NewLogManager(fileManager, "logfile.log")
	bufferManager := bm.NewBufferManager(fileManager, logManager, 3)

	tx1 := tx.NewTransaction(fileManager, logManager, bufferManager)
	sch := rm.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)

	mdm := NewMetaDataManager(true, tx1)
	mdm.CreateTable("MyTable", sch, tx1)
	layout := mdm.GetLayout("MyTable", tx1)
	size := layout.SlotSize()
	fmt.Printf("MyTable has slot size: %d\n", size)
	sch2 := layout.Schema()
	fmt.Println("Its fields are: ")
	for _, fldName := range sch2.Fields() {
		fldType := ""
		if sch2.Type(fldName) == rm.INTEGER {
			fldType = "int"
		} else {
			strlen := sch2.Length(fldName)
			fldType = fmt.Sprintf("varchar ( %d )", strlen)
		}

		fmt.Printf("%s : %s\n", fldName, fldType)
	}

	ts := rm.NewTableScan(tx1, "MyTable", layout)
	//测试统计元数据
	for i := 0; i < 50; i++ {
		ts.Insert()
		n := rand.Intn(50)
		ts.SetInt("A", n)
		strField := fmt.Sprintf("rec%d", n)
		ts.SetString("B", strField)
	}
	si := mdm.GetStatInfo("MyTable", layout, tx1)
	fmt.Printf("blocks for MyTable is %d\n", si.BlocksAccessed())
	fmt.Printf("records for MyTable is :%d\n", si.RecordsOutput())
	fmt.Printf("Distinc values for field A is %d\n", si.DistinctValues("A"))
	fmt.Printf("Distinc values for field B is %d\n", si.DistinctValues("B"))

	//统计视图信息
	viewDef := "select B from MyTable where A = 1"
	mdm.CreateView("viewA", viewDef, tx1)
	v := mdm.GetViewDef("viewA", tx1)
	fmt.Printf("View def = %s\n", v)
	tx1.Commit()
}
