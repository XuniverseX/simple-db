package file_manager

import "encoding/binary"

type Page struct {
	buffer []byte
}

func NewPageBySize(blockSize uint64) *Page {
	bytes := make([]byte, blockSize)
	return &Page{
		buffer: bytes,
	}
}

func NewPageByBytes(bytes []byte) *Page {
	return &Page{
		buffer: bytes,
	}
}

func (p *Page) GetInt(offset uint64) uint64 {
	return binary.LittleEndian.Uint64(p.buffer[offset : offset+8])
}

func (p *Page) SetInt(offset uint64, val uint64) {
	bytes := Uint64ToByteArray(val)
	copy(p.buffer[offset:], bytes)
}

func Uint64ToByteArray(val uint64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, val)
	return bytes
}

func (p *Page) GetBytes(offset uint64) []byte {
	length := binary.LittleEndian.Uint64(p.buffer[offset : offset+8]) //偏移量处的字节表示后面字节数组的length
	newBuf := make([]byte, length)
	copy(newBuf, p.buffer[offset+8:])
	return newBuf
}

func (p *Page) SetBytes(offset uint64, b []byte) {
	//先写入数组长度，再写入数组内容
	length := uint64(len(b))
	lenBytes := Uint64ToByteArray(length) //长度的字节数组表示
	copy(p.buffer[offset:], lenBytes)     //在偏移量处写入字节数组长度
	copy(p.buffer[offset+8:], b)
}

func (p *Page) GetString(offset uint64) string {
	strBytes := p.GetBytes(offset)
	return string(strBytes)
}

func (p *Page) SetString(offset uint64, s string) {
	strBytes := []byte(s)
	p.SetBytes(offset, strBytes)
}

func MaxLengthOfStringInPage(s string) uint64 {
	bs := []byte(s)
	return uint64(8 + len(bs)) //字节数组长度+字符串真实长度
}

func (p *Page) Contents() []byte {
	return p.buffer
}
