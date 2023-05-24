package record_manager

import (
	fm "file_manager"
	"tx"
)

const (
	BYTES_OF_INT = 8
)

type Layout struct {
	schema   SchemaInterface
	offsets  map[string]int //字段在记录中的偏移
	slotSize int            //这条记录的长度
}

func NewLayout(schema SchemaInterface, offsets map[string]int, slotSize int) *Layout {
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}

}

func NewLayoutWithSchema(schema SchemaInterface) *Layout {
	layout := NewLayout(schema, make(map[string]int), 0)
	fields := schema.Fields()
	pos := tx.UINT64_LENGTH //标志位
	for i := 0; i < len(fields); i++ {
		layout.offsets[fields[i]] = pos
		pos += layout.LengthInBytes(fields[i])
	}
	layout.slotSize = pos
	return layout
}

func (l *Layout) Schema() SchemaInterface {
	return l.schema
}

func (l *Layout) Offset(fieldName string) int {
	offset, ok := l.offsets[fieldName]
	if !ok {
		return -1
	}
	return offset
}

func (l *Layout) SlotSize() int {
	return l.slotSize
}

func (l *Layout) LengthInBytes(fieldName string) int {
	fldType := l.schema.Type(fieldName)
	if fldType == INTEGER {
		return BYTES_OF_INT
	} else {
		fldLen := l.schema.Length(fieldName) //获取字段内容的长度
		/**
		 * 因为是varchar类型，我们根据长度构造一个字符串，然后调用fm.MaxLengthForString
		 * 获得写入页面时的数据长度，回忆一下在将字符串数据写入页面时，我们需要先写入8个字节用于记录
		 * 写入字符串的长度
		 */
		dummy := string(make([]byte, fldLen))
		return int(fm.MaxLengthOfStringInPage(dummy))
	}
}
