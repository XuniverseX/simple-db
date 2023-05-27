package metadata_manager

import (
	bm "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	rm "record_manager"
	"testing"
	"tx"
)

func TestTableManager(t *testing.T) {
	fileManager, _ := fm.NewFileManager("recordtest", 400)
	logManager, _ := lm.NewLogManager(fileManager, "logfile.log")
	bufferManager := bm.NewBufferManager(fileManager, logManager, 3)

	tx1 := tx.NewTransaction(fileManager, logManager, bufferManager)
	sch := rm.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)

	tableManager := NewTableManager(true, tx1)
	tableManager.CreateTable("MyTable", sch, tx1)
	layout := tableManager.GetLayout("MyTable", tx1)
	size := layout.SlotSize()
	sch2 := layout.Schema()
	fmt.Printf("MyTable has slot size: %d\n", size)
	fmt.Println("Its fields are: ")
	for _, fldName := range sch2.Fields() {
		fldType := ""
		if sch2.Type(fldName) == rm.INTEGER {
			fldType = "int"
		} else {
			strlen := sch2.Length(fldName)
			fldType = fmt.Sprintf("varchar( %d )", strlen)
		}
		fmt.Printf("%s : %s\n", fldName, fldType)
	}

	tx1.Commit()

}
