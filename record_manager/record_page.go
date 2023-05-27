package record_manager

import (
	fm "file_manager"
	"fmt"
	"tx"
)

type SLOT_FLAG int

const (
	EMPTY SLOT_FLAG = iota
	USED
)

type RecordPage struct {
	tx     *tx.Transaction
	blk    *fm.BlockId
	layout LayoutInterface
}

func NewRecordPage(tx *tx.Transaction, blk *fm.BlockId, layout LayoutInterface) *RecordPage {
	rp := &RecordPage{
		tx:     tx,
		blk:    blk,
		layout: layout,
	}
	tx.Pin(blk)

	return rp
}

func (r *RecordPage) Block() *fm.BlockId {
	return r.blk
}

func (r *RecordPage) GetInt(slot int, fieldName string) int {
	fldPos := r.slotOffset(slot) + uint64(r.layout.Offset(fieldName))
	val, err := r.tx.GetInt(r.blk, fldPos)
	if err != nil {
		return -1
	}

	return int(val)
}

func (r *RecordPage) SetInt(slot int, fieldName string, val int) {
	fldPos := r.slotOffset(slot) + uint64(r.layout.Offset(fieldName))
	r.tx.SetInt(r.blk, fldPos, int64(val), true)
}

func (r *RecordPage) GetString(slot int, fieldName string) string {
	fldPos := r.slotOffset(slot) + uint64(r.layout.Offset(fieldName))
	val, _ := r.tx.GetString(r.blk, fldPos)
	return val
}

func (r *RecordPage) SetString(slot int, fieldName string, val string) {
	fieldPos := r.slotOffset(slot) + uint64(r.layout.Offset(fieldName))
	r.tx.SetString(r.blk, fieldPos, val, true)
}

func (r *RecordPage) Format() {
	slot := 0
	for r.isValidSlot(slot) {
		r.tx.SetInt(r.blk, r.slotOffset(slot), int64(EMPTY), false)
		sch := r.layout.Schema()
		for _, fieldName := range sch.Fields() {
			fldPos := r.slotOffset(slot) + uint64(r.layout.Offset(fieldName))
			if sch.Type(fieldName) == INTEGER {
				r.tx.SetInt(r.blk, fldPos, 0, false)
			} else {
				r.tx.SetString(r.blk, fldPos, "", false)
			}
			slot++
		}
	}
}

func (r *RecordPage) Delete(slot int) {
	r.setFlag(slot, EMPTY)
}

func (r *RecordPage) InsertAfter(slot int) int {
	newSlot := r.searchAfter(slot, EMPTY)
	if newSlot >= 0 {
		r.setFlag(newSlot, USED)
	}
	return newSlot
}

func (r *RecordPage) NextAfter(slot int) int {
	return r.searchAfter(slot, USED)
}

func (r *RecordPage) slotOffset(slot int) uint64 {
	return uint64(slot * r.layout.SlotSize())
}

func (r *RecordPage) setFlag(slot int, flag SLOT_FLAG) {
	r.tx.SetInt(r.blk, r.slotOffset(slot), int64(flag), true)
}

func (r *RecordPage) isValidSlot(slot int) bool {
	return r.slotOffset(slot+1) <= r.tx.BlockSize()
}

func (r *RecordPage) searchAfter(slot int, flag SLOT_FLAG) int {
	slot++
	for r.isValidSlot(slot) {
		val, err := r.tx.GetInt(r.blk, r.slotOffset(slot))
		if err != nil {
			fmt.Printf("SearchAfter has err %v\n", err)
			return -1
		}

		if SLOT_FLAG(val) == flag {
			return slot
		}
		slot++
	}

	return -1
}
