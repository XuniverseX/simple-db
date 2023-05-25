package record_manager

import (
	fm "file_manager"
	"tx"
)

type TableScan struct {
	tx       *tx.Transaction
	layout   LayoutInterface
	rp       RecordManagerInterface
	fileName string
	currSlot int
}

func NewTableScan(tx *tx.Transaction, tableName string, layout LayoutInterface) *TableScan {
	t := &TableScan{
		tx:       tx,
		layout:   layout,
		fileName: tableName + ".tbl",
	}

	size, err := tx.Size(t.fileName)
	if err != nil {
		panic(err)
	}

	if size == 0 {
		//文件为空，增加一个区块
		t.MoveToNewBlock()
	} else {
		//读取第一个区块
		t.MoveToBlock(0)
	}

	return t
}

func (t *TableScan) Close() {
	if t.rp != nil {
		t.tx.Unpin(t.rp.Block())
	}
}

func (t *TableScan) BeforeFirst() {
	t.MoveToBlock(0)
}

func (t *TableScan) Next() bool {
	//如果在当前区块找不到给定有效记录则遍历后续区块，直到所有区块都遍历为止
	t.currSlot = t.rp.NextAfter(t.currSlot)
	for t.currSlot < 0 {
		if t.AtLastBlock() {
			//直到最后一个区块都没有可用插槽
			return false
		}
		t.MoveToBlock(int(t.rp.Block().Number() + 1))
		t.currSlot = t.rp.NextAfter(t.currSlot)
	}

	return true
}

func (t *TableScan) HasField(fieldName string) bool {
	return t.layout.Schema().HasFields(fieldName)
}

func (t *TableScan) MoveToRid(r RIDInterface) {
	t.Close()
	blk := fm.NewBlockId(t.fileName, uint64(r.BlockNumber()))
	t.rp = NewRecordPage(t.tx, blk, t.layout)
	t.currSlot = r.Slot()
}

// Insert 将当前插槽指向下一个可用插槽
func (t *TableScan) Insert() {
	t.currSlot = t.rp.InsertAfter(t.currSlot)
	for t.currSlot < 0 { //当前区块找不到可用插槽
		if t.AtLastBlock() {
			//如果当前处于最后一个区块，那么新增一个区块
			t.MoveToNewBlock()
		} else {
			t.MoveToBlock(int(t.rp.Block().Number() + 1))
		}

		t.currSlot = t.rp.InsertAfter(t.currSlot)
	}
}

func (t *TableScan) GetInt(fieldName string) int {
	return t.rp.GetInt(t.currSlot, fieldName)
}

func (t *TableScan) GetString(fieldName string) string {
	return t.rp.GetString(t.currSlot, fieldName)
}

func (t *TableScan) SetInt(fieldName string, val int) {
	t.rp.SetInt(t.currSlot, fieldName, val)
}

func (t *TableScan) SetString(fieldName string, val string) {
	t.rp.SetString(t.currSlot, fieldName, val)
}

func (t *TableScan) GetRid() RIDInterface {
	return NewRID(int(t.rp.Block().Number()), t.currSlot)
}

func (t *TableScan) Delete() {
	t.rp.Delete(t.currSlot)
}

func (t *TableScan) GetVal(fieldName string) *Constant {
	if t.layout.Schema().Type(fieldName) == INTEGER {
		return NewConstantWithInt(t.GetInt(fieldName))
	}

	return NewConstantWithString(t.GetString(fieldName))
}

func (t *TableScan) SetVal(fieldName string, val *Constant) {
	if t.layout.Schema().Type(fieldName) == INTEGER {
		t.SetInt(fieldName, val.iVal)
	} else {
		t.SetString(fieldName, val.sVal)
	}
}

func (t *TableScan) MoveToBlock(blkNum int) {
	t.Close()
	blk := fm.NewBlockId(t.fileName, uint64(blkNum))
	t.rp = NewRecordPage(t.tx, blk, t.layout)
	t.currSlot = -1
}

func (t *TableScan) AtLastBlock() bool {
	size, err := t.tx.Size(t.fileName)
	if err != nil {
		panic(err)
	}

	return t.rp.Block().Number() == size-1
}

func (t *TableScan) MoveToNewBlock() {
	t.Close()
	blk, err := t.tx.Append(t.fileName)
	if err != nil {
		panic(err)
	}

	t.rp = NewRecordPage(t.tx, blk, t.layout)
	t.rp.Format()
	t.currSlot = -1
}
