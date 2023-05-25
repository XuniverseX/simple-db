package record_manager

import (
	bm "buffer_manager"
	fm "file_manager"
	"fmt"
	"github.com/stretchr/testify/require"
	lm "log_manager"
	"math/rand"
	"testing"
	"tx"
)

func TestTableScanInsertAndDelete(t *testing.T) {
	fileManager, _ := fm.NewFileManager("recordtest", 400)
	logManager, _ := lm.NewLogManager(fileManager, "logfile.log")
	bufferManager := bm.NewBufferManager(fileManager, logManager, 3)

	tx1 := tx.NewTransaction(fileManager, logManager, bufferManager)
	sch := NewSchema()

	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := NewLayoutWithSchema(sch)
	for _, fieldName := range layout.Schema().Fields() {
		offset := layout.Offset(fieldName)
		fmt.Printf("%s has offset %d\n", fieldName, offset)
	}

	ts := NewTableScan(tx1, "T", layout)
	fmt.Println("Filling the table with 50 random records")
	ts.BeforeFirst()
	valForFieldA := make([]int, 0)
	for i := 0; i < 50; i++ {
		ts.Insert() //指向一个可用插槽
		n := rand.Intn(50)
		ts.SetInt("A", n)
		valForFieldA = append(valForFieldA, n)
		s := fmt.Sprintf("rec%d", n)
		ts.SetString("B", s)
		fmt.Printf("inserting into slot%s:{%d , %s}\n", ts.GetRid().ToString(), n, s)
	}

	ts.BeforeFirst()
	//测试插入的内容是否正确
	slot := 0
	for ts.Next() {
		a := ts.GetInt("A")
		b := ts.GetString("B")
		require.Equal(t, a, valForFieldA[slot])
		require.Equal(t, b, fmt.Sprintf("rec%d", a))
		slot++
	}

	fmt.Println("Deleting records with A-values < 25")
	count := 0
	ts.BeforeFirst()
	for ts.Next() {
		a := ts.GetInt("A")
		b := ts.GetString("B")
		if a < 25 {
			count += 1
			fmt.Printf("slot%s:{%d , %s}\n", ts.GetRid().ToString(), a, b)
			ts.Delete()
		}
	}

	fmt.Println("Here are the remaining records:")
	ts.BeforeFirst()
	for ts.Next() {
		a := ts.GetInt("A")
		b := ts.GetString("B")
		require.Equal(t, a >= 25, true)
		fmt.Printf("slot%s:{%d , %s}\n", ts.GetRid().ToString(), a, b)
	}

	ts.Close()
	tx1.Commit()
}
