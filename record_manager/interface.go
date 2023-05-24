package record_manager

import fm "file_manager"

// SchemaInterface 表的模式
type SchemaInterface interface {
	AddField(fieldName string, fieldType FIELD_TYPE, length int)
	AddIntField(fieldName string)
	AddStringField(fieldName string, length int)
	Add(fieldName string, sch SchemaInterface)
	AddAll(sch SchemaInterface)
	Fields() []string
	HasFields(fieldName string) bool
	Type(fieldName string) FIELD_TYPE
	Length(fieldName string) int
}

// LayoutInterface 记录字段偏移等信息
type LayoutInterface interface {
	Schema() SchemaInterface
	Offset(fieldName string) int //所在记录（行）中字段（列）的偏移
	SlotSize() int
}

type RecordManager interface {
	Block() *fm.BlockId                               //返回记录所在页面对应的区块
	GetInt(slot int, fieldName string) int            //根据给定字段名取出其对应的int值
	SetInt(slot int, fieldName string, val int)       //设定指定字段名的int值
	GetString(slot int, fieldName string) string      //根据给定字段名获取其字符串内容
	SetString(slot int, fieldName string, val string) //设置给定字段名的字符串内容
	Format()                                          //将所有页面中的记录设置为默认值
	Delete(slot int)                                  //删除给定编号的记录
	InvalidAfterSlot(slot int) int                    //查找给定插槽之后第一个占用标志位为1的记录
	ValidAfterSlot(slot int) int                      //查找给定插槽之后第一个占用标志位为0的记录
}