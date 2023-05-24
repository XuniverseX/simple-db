package record_manager

import (
	"github.com/stretchr/testify/require"
	"testing"
	"tx"
)

func TestLayoutOffset(t *testing.T) {
	sch := NewSchema()
	sch.AddIntField("id")
	sch.AddStringField("name", 9)
	sch.AddIntField("classNum")
	layout := NewLayoutWithSchema(sch)
	fields := sch.Fields()
	/**
	 * 字段id前面用一个int做占用标志位，因此字段id的偏移是8，
	 * 字段id的类型是int,在go中该类型长度为8，因此字段name的偏移就是16
	 * 字段name是字符串类型，它的偏移是16，存入page时会先存入8字节来记录字符串的长度，它自身长度为9
	 * 因此字段classNum的偏移是16+8+9=33
	 */
	idOff := layout.Offset(fields[0])
	require.Equal(t, tx.UINT64_LENGTH, idOff)

	nameOff := layout.Offset(fields[1])
	require.Equal(t, 16, nameOff)

	cnOff := layout.Offset(fields[2])
	require.Equal(t, 33, cnOff)
}
