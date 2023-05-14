package file_manager

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFileManager(t *testing.T) {
	fm, _ := NewFileManager("file_test", 400)

	blk := NewBlockId("testBlk", 2)
	p1 := NewPageBySize(fm.BlockSize())
	pos1 := uint64(88)
	s := "abcdefghijkfdasfsad"
	p1.SetString(pos1, s)
	sLen := p1.MaxLengthOfString(s)
	pos2 := pos1 + sLen
	val := uint64(345)
	p1.SetInt(pos2, val)

	fm.Write(blk, p1)

	p2 := NewPageBySize(fm.BlockSize())
	fm.Read(blk, p2)

	require.Equal(t, val, p2.GetInt(pos2))
	require.Equal(t, s, p2.GetString(pos1))
}
