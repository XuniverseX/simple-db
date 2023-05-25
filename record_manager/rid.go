package record_manager

import "fmt"

type RID struct {
	blkNum int
	slot   int
}

func NewRID(blkNum int, slot int) *RID {
	return &RID{
		blkNum: blkNum,
		slot:   slot,
	}
}

func (r *RID) BlockNumber() int {
	return r.blkNum
}

func (r *RID) Slot() int {
	return r.slot
}

func (r *RID) Equals(other RIDInterface) bool {
	return r.blkNum == other.BlockNumber() && r.slot == other.Slot()
}

func (r *RID) ToString() string {
	return fmt.Sprintf("[%d, %d]", r.blkNum, r.slot)
}
