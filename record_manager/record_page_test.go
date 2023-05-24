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

func TestRecordPageInsertAndDelete(t *testing.T) {
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

	blk, err := tx1.Append("testfile")
	require.Nil(t, err)

	tx1.Pin(blk)
	rp := NewRecordPage(tx1, blk, layout)
	rp.Format()
	fmt.Println("Filling the page with random records")
	slot := rp.ValidAfterSlot(-1) //找到第一条可用插槽
	valForFieldA := make([]int, 0)
	for slot >= 0 {
		n := rand.Intn(50)
		valForFieldA = append(valForFieldA, n)
		rp.SetInt(slot, "A", n)                          //找到可用插槽后随机设定字段A的值
		rp.SetString(slot, "B", fmt.Sprintf("rec%d", n)) //设定字段B
		fmt.Printf("inserting into slot%d {%d , rec%d}\n", slot, n, n)
		slot = rp.ValidAfterSlot(slot) //查找当前插槽之后可用的插槽
	}

	slot = rp.InvalidAfterSlot(-1) //测试插入字段是否正确
	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")
		require.Equal(t, a, valForFieldA[slot])
		require.Equal(t, b, fmt.Sprintf("rec%d", a))
		slot = rp.InvalidAfterSlot(slot)
	}

	fmt.Println("Deleted these records with A-values < 25.")
	count := 0
	slot = rp.InvalidAfterSlot(-1)
	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")
		if a < 25 {
			count++
			fmt.Printf("slot %d: {%d, %s}\n", slot, a, b)
			rp.Delete(slot)
		}
		slot = rp.InvalidAfterSlot(slot)
	}
	fmt.Printf("%d values under 25 were deleted.\n", count)
	fmt.Println("Here are the remaining records")
	slot = rp.InvalidAfterSlot(-1)
	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")

		require.Equal(t, a >= 25, true)

		fmt.Printf("slot%d {%d, %s}\n", slot, a, b)
		slot = rp.InvalidAfterSlot(slot)
	}

	tx1.Unpin(blk)
	tx1.Commit()
}
