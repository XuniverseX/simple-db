package file_manager

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGetAndSetInt(t *testing.T) {
	// require.Equal(t, 1, 2)
	page := NewPageBySize(256)
	val := uint64(1234)
	offset := uint64(23)
	page.SetInt(offset, val)
	valGot := page.GetInt(offset)
	require.Equal(t, val, valGot)
}

func TestGetAndSetByteArray(t *testing.T) {
	// require.Equal(t, 1, 2)
	page := NewPageBySize(256)
	bs := []byte{1, 2, 3, 4, 5}
	offset := uint64(111)
	page.SetBytes(offset, bs)
	bsGot := page.GetBytes(offset)
	require.Equal(t, bs, bsGot)
}

func TestGetAndSetString(t *testing.T) {
	// require.Equal(t, 1, 2)
	page := NewPageBySize(256)
	str := "hello,世界"
	offset := uint64(177)
	page.SetString(offset, str)
	strGot := page.GetString(offset)
	require.Equal(t, str, strGot)
}

func TestMaxLengthOfString(t *testing.T) {
	s := "hello,世界"
	sLen := uint64(len([]byte(s)))
	page := NewPageBySize(256)
	sLenGot := page.MaxLengthOfString(s)
	require.Equal(t, sLen+8, sLenGot)
}

func TestContents(t *testing.T) {
	bs := []byte{1, 2, 3, 4, 5, 6}
	page := NewPageByBytes(bs)
	bsGot := page.Contents()
	require.Equal(t, bs, bsGot)
}
