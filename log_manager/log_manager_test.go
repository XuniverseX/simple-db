package log_manager

import (
	fm "file_manager"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

const (
	DIR          = "log_test"
	LOG_FILENAME = "logfile"
)

func makeRecord(s string, n uint64) []byte {
	//生成日志内容
	p := fm.NewPageBySize(1)
	nPos := p.MaxLengthOfString(s)
	b := make([]byte, nPos+UINT64_LEN)
	p = fm.NewPageByBytes(b)
	p.SetString(0, s)
	p.SetInt(nPos, n)

	return b
}

func createRecords(lm *LogManager, start uint64, end uint64) {
	for i := start; i <= end; i++ {
		rec := makeRecord(fmt.Sprintf("record%d", i), i)
		lm.Append(rec)
	}
}

func TestLogManager(t *testing.T) {
	fileManager, _ := fm.NewFileManager(DIR, 400)
	logManager, err := NewLogManager(fileManager, LOG_FILENAME)
	require.Nil(t, err)

	createRecords(logManager, 1, 35)

	iter := logManager.Iterator()
	recNum := uint64(35)
	for iter.HasNext() {
		rec := iter.Next()
		p := fm.NewPageByBytes(rec)
		s := p.GetString(0)
		require.Equal(t, fmt.Sprintf("record%d", recNum), s)

		nPos := p.MaxLengthOfString(s)
		val := p.GetInt(nPos)
		require.Equal(t, recNum, val)

		recNum--
	}

	createRecords(logManager, 36, 70)
	logManager.FlushByLSN(65)

	iter = logManager.Iterator()
	recNum = uint64(70)
	for iter.HasNext() {
		rec := iter.Next()
		p := fm.NewPageByBytes(rec)
		s := p.GetString(0)
		require.Equal(t, fmt.Sprintf("record%d", recNum), s)

		nPos := p.MaxLengthOfString(s)
		val := p.GetInt(nPos)
		require.Equal(t, recNum, val)

		recNum--
	}
}
