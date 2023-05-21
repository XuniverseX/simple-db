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

func (t *Sub) Commit() error {
	return nil
}

func (t *Sub) Rollback() error {
	return nil
}

func (t *Sub) Recover() error {
	return nil
}

func (t *Sub) Pin(_ *fm.BlockId) error {
	return nil
}

func (t *Sub) Unpin(_ *fm.BlockId) {

}
func (t *Sub) GetInt(_ *fm.BlockId, offset uint64) (int64, error) {
	return int64(t.p.GetInt(offset)), nil
}

func (t *Sub) GetString(_ *fm.BlockId, offset uint64) (string, error) {
	val := t.p.GetString(offset)
	return val, nil
}

func (t *Sub) SetInt(_ *fm.BlockId, offset uint64, val int64, _ bool) error {
	t.p.SetInt(offset, uint64(val))
	return nil
}

func (t *Sub) SetString(_ *fm.BlockId, offset uint64, val string, _ bool) error {
	t.p.SetString(offset, val)
	return nil
}

func (t *Sub) AvailableBuffers() uint64 {
	return 0
}

func (t *Sub) Size(_ string) (uint64, error) {
	return 0, nil
}

func (t *Sub) Append(_ string) (*fm.BlockId, error) {
	return nil, nil
}

func (t *Sub) BlockSize() uint64 {
	return 0
}
