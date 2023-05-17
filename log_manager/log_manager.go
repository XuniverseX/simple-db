package log_manager

import (
	"sync"

	fm "file_manager"
)

const (
	UINT64_LEN = 8
)

type LogManager struct {
	fileManager  *fm.FileManager
	logFile      string      //日志文件的名称
	logPage      *fm.Page    //存储日志的缓冲区
	currentBlk   *fm.BlockId //日志当前写入的区块号
	latestLsn    uint64      //当前最新日志编号
	lastSavedLsn uint64      //上一次写入磁盘的日志编号
	mu           sync.Mutex
}

// 缓冲区用完时调用此接口分配新内存
func (l *LogManager) appendNewBlock() (*fm.BlockId, error) {
	blk, err := l.fileManager.Append(l.logFile) //在日志二进制末尾添加一个区块
	if err != nil {
		return nil, err
	}

	//日志写入是从底向上写，先在头8字节写入缓冲区大小
	l.logPage.SetInt(0, l.fileManager.BlockSize()) //缓冲区大小
	l.fileManager.Write(&blk, l.logPage)           //TODO: 此处写入时会写入旧Page的数据

	return &blk, nil
}

func NewLogManager(fileManager *fm.FileManager, logFile string) (*LogManager, error) {
	logMgr := LogManager{
		fileManager:  fileManager,
		logFile:      logFile,
		logPage:      fm.NewPageBySize(fileManager.BlockSize()),
		latestLsn:    0,
		lastSavedLsn: 0,
	}

	logSize, err := fileManager.Size(logFile)
	if err != nil {
		return nil, err
	}

	if logSize == 0 {
		//如果文件为空，就要添加一个新区块给这个文件
		blk, err := logMgr.appendNewBlock()
		if err != nil {
			return nil, err
		}

		logMgr.currentBlk = blk
	} else {
		//如果文件存在，先把末尾的文件内容读入内存，如果还有空间，新的日志就写入当前区块
		logMgr.currentBlk = fm.NewBlockId(logFile, logSize-1)
		fileManager.Read(logMgr.currentBlk, logMgr.logPage)
	}

	return &logMgr, nil
}

// FlushByLSN 把给定编号及之前的数据写入磁盘 LSN -> log sequence number
func (l *LogManager) FlushByLSN(lsn uint64) error {
	// 除了该日志编号之前的数据，当前区块的日志也会被写入磁盘
	if lsn > l.lastSavedLsn {
		err := l.Flush()
		if err != nil {
			return err
		}

		l.lastSavedLsn = lsn
	}
	return nil
}

// Flush 将给定缓冲区数据写入磁盘
func (l *LogManager) Flush() error {
	_, err := l.fileManager.Write(l.currentBlk, l.logPage)
	if err != nil {
		return err
	}

	return nil
}

// Append 添加日志
func (l *LogManager) Append(logRecord []byte) (uint64, error) {
	boundary := l.logPage.GetInt(0) //获取可写入的底部偏移
	recordSize := uint64(len(logRecord))
	bytesNeed := recordSize + UINT64_LEN

	if int(boundary-bytesNeed) < UINT64_LEN {
		//缓冲区空间不足，将当前缓冲区数据写入磁盘
		err := l.Flush()
		if err != nil {
			return l.latestLsn, err
		}

		//生成新区块用于写入新数据
		l.currentBlk, err = l.appendNewBlock()
		if err != nil {
			return l.latestLsn, err
		}

		boundary = l.logPage.GetInt(0) //获取appendNewBlock()新写入的偏移值
	}

	recordPos := boundary - bytesNeed //从底部往上写入
	l.logPage.SetBytes(recordPos, logRecord)
	l.logPage.SetInt(0, recordPos) //重新设置可写入偏移
	l.latestLsn++                  //记录新加入日志的编号

	return l.latestLsn, nil
}

func (l *LogManager) Iterator() *LogIterator {
	//生成日志遍历器
	l.Flush()
	return NewLogIterator(l.fileManager, l.currentBlk)
}
