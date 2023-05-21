package file_manager

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type FileManager struct {
	dbDirectory string
	blockSize   uint64
	isNew       bool
	openFiles   map[string]*os.File
	mu          sync.Mutex
}

func NewFileManager(dbDirectory string, blockSize uint64) (*FileManager, error) {
	fileManager := FileManager{
		dbDirectory: dbDirectory,
		blockSize:   blockSize,
		isNew:       false,
		openFiles:   make(map[string]*os.File),
	}

	if _, err := os.Stat(dbDirectory); os.IsNotExist(err) {
		//目录不存在则生成
		fileManager.isNew = true
		//err := os.Mkdir(dbDirectory, os.ModeDir) //正式run开启这一行，注释下面
		err := os.Mkdir(dbDirectory, os.ModePerm) //debug时开启这一行，权限0777
		if err != nil {
			return nil, err
		}
	} else {
		//如果目录已经存在，删除目录下的临时文件
		err := filepath.Walk(dbDirectory, func(path string, info fs.FileInfo, err error) error {
			mode := info.Mode()
			if mode.IsRegular() {
				name := info.Name()
				if strings.HasPrefix(name, "temp") {
					//删除临时文件
					os.Remove(filepath.Join(path, name))
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return &fileManager, nil
}

func (f *FileManager) getFile(filename string) (*os.File, error) {
	path := filepath.Join(f.dbDirectory, filename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	f.openFiles[path] = file
	return file, nil
}

func (f *FileManager) Read(blk *BlockId, p *Page) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := f.getFile(blk.FileName())
	if err != nil {
		return 0, err
	}

	defer file.Close()

	n, err := file.ReadAt(p.Contents(), int64(blk.Number()*f.blockSize))
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (f *FileManager) Write(blk *BlockId, p *Page) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := f.getFile(blk.FileName())
	if err != nil {
		return 0, err
	}

	defer file.Close()

	n, err := file.WriteAt(p.Contents(), int64(blk.Number()*f.blockSize))
	if err != nil {
		return 0, err
	}

	return n, nil
}

// Size 返回f中包含多少个区块
func (f *FileManager) Size(fileName string) (uint64, error) {
	file, err := f.getFile(fileName)
	if err != nil {
		return 0, err
	}

	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return 0, err
	}

	return uint64(fi.Size()) / f.blockSize, nil
}

func (f *FileManager) Append(fileName string) (BlockId, error) {
	newBlockNum, err := f.Size(fileName) //区块号从0开始，相当于blkNum加1
	if err != nil {
		return BlockId{}, err
	}

	blk := NewBlockId(fileName, newBlockNum)
	file, err := f.getFile(fileName)
	if err != nil {
		return BlockId{}, err
	}

	b := make([]byte, f.blockSize)
	_, err = file.WriteAt(b, int64(blk.Number()*f.blockSize)) //在文件末尾写入新区块
	if err != nil {
		return BlockId{}, err
	}

	return *blk, nil
}

func (f *FileManager) IsNew() bool {
	return f.isNew
}

func (f *FileManager) BlockSize() uint64 {
	return f.blockSize
}
