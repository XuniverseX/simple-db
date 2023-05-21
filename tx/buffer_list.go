package tx

import (
	bm "buffer_manager"
	fm "file_manager"
)

// BufferList 管理当前被Pin的内存页面
type BufferList struct {
	buffers       map[*fm.BlockId]*bm.Buffer
	bufferManager *bm.BufferManager
	pins          []*fm.BlockId
}

func NewBufferList(bufferManager *bm.BufferManager) *BufferList {
	return &BufferList{
		buffers:       make(map[*fm.BlockId]*bm.Buffer),
		bufferManager: bufferManager,
		pins:          make([]*fm.BlockId, 0),
	}
}

func (b *BufferList) getBuffer(blk *fm.BlockId) *bm.Buffer {
	buff := b.buffers[blk]
	return buff
}

func (b *BufferList) Pin(blk *fm.BlockId) error {
	//如果给定页面被pin，他会加入map进行管理
	buff, err := b.bufferManager.Pin(blk)
	if err != nil {
		return err
	}

	b.buffers[blk] = buff
	b.pins = append(b.pins, blk)

	return nil
}

func (b *BufferList) Unpin(blk *fm.BlockId) {
	buff, ok := b.buffers[blk]
	if !ok {
		return
	}

	b.bufferManager.Unpin(buff)

	for i, blockId := range b.pins {
		if blockId.Equal(blk) {
			b.pins = append(b.pins[:i], b.pins[i+1:]...) //从数组中删除
			break
		}
	}

	delete(b.buffers, blk) //从map中删除
}

func (b *BufferList) UnpinAll() {
	for _, blk := range b.pins {
		buff := b.buffers[blk]
		b.bufferManager.Unpin(buff)
	}

	b.buffers = make(map[*fm.BlockId]*bm.Buffer)
	b.pins = make([]*fm.BlockId, 0)
}
