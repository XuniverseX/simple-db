package buffer_manager

import (
	fm "file_manager"
	"fmt"
	"github.com/stretchr/testify/require"
	lm "log_manager"
	"testing"
)

const (
	DIR            = "buffer_test"
	LOG_FILENAME   = "logfile"
	BLOCK_FILENAME = "testfile"
)

func TestBufferManager(t *testing.T) {
	fileManager, _ := fm.NewFileManager(DIR, 400)
	logManager, _ := lm.NewLogManager(fileManager, LOG_FILENAME)
	bufferManager := NewBufferManager(fileManager, logManager, 3)

	buff1, err := bufferManager.Pin(fm.NewBlockId(BLOCK_FILENAME, 1))
	require.Nil(t, err)

	p := buff1.Contents()
	//_ := p.GetInt(80)
	p.SetInt(80, 99999)
	buff1.SetModified(1, 0) //通知缓存管理器 数据被修改了
	bufferManager.Unpin(buff1)

	buff2, err := bufferManager.Pin(fm.NewBlockId(BLOCK_FILENAME, 2))
	require.Nil(t, err)

	_, err = bufferManager.Pin(fm.NewBlockId(BLOCK_FILENAME, 3))
	require.Nil(t, err)

	_, err = bufferManager.Pin(fm.NewBlockId(BLOCK_FILENAME, 4)) //促使buff1将数据写入磁盘
	require.Nil(t, err)

	bufferManager.Unpin(buff2)
	buff2, err = bufferManager.Pin(fm.NewBlockId(BLOCK_FILENAME, 1))
	require.Nil(t, err)

	p2 := buff2.Contents()
	p2.SetInt(80, 9999)
	buff2.SetModified(1, 0)
	bufferManager.Unpin(buff2) //注意这里不会将buff2的数据写入磁盘

	//将testfile的区块1读入，并确认buff1的数据的确写入磁盘
	page := fm.NewPageBySize(400)
	b1 := fm.NewBlockId(BLOCK_FILENAME, 1)
	fileManager.Read(b1, page)
	n1 := page.GetInt(80)
	fmt.Println(n1)
	require.Equal(t, uint64(99999), n1)
}
