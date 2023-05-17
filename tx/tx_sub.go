package tx

import (
	fm "file_manager"
)

type Sub struct {
	p *fm.Page
}

func NewSub(p *fm.Page) *Sub {
	return &Sub{
		p: p,
	}
}

func (t *Sub) Commit() {

}

func (t *Sub) Rollback() {

}

func (t *Sub) Recover() {

}

func (t *Sub) Pin(_ *fm.BlockId) {

}

func (t *Sub) Unpin(_ *fm.BlockId) {

}
func (t *Sub) GetInt(_ *fm.BlockId, offset uint64) uint64 {
	return t.p.GetInt(offset)
}

func (t *Sub) GetString(_ *fm.BlockId, offset uint64) string {
	val := t.p.GetString(offset)
	return val
}

func (t *Sub) SetInt(_ *fm.BlockId, offset uint64, val uint64, _ bool) {
	t.p.SetInt(offset, val)
}

func (t *Sub) SetString(_ *fm.BlockId, offset uint64, val string, _ bool) {
	t.p.SetString(offset, val)
}

func (t *Sub) Available() uint64 {
	return 0
}

func (t *Sub) Size(_ string) uint64 {
	return 0
}

func (t *Sub) Append(_ string) *fm.BlockId {
	return nil
}

func (t *Sub) BlockSize() uint64 {
	return 0
}
