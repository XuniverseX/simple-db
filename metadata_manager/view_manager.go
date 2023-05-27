package metadata_manager

import (
	rm "record_manager"
	"tx"
)

// viewcat(viewName, viewDef 生成视图表的sql语句)

const (
	// MAX_VIEWDEF 用于创建视图的SQL语句的最大长度
	MAX_VIEWDEF = 100
)

type ViewManager struct {
	tblMgr *TableManager
}

func NewViewMananger(isNew bool, tblMgr *TableManager, tx *tx.Transaction) *ViewManager {
	viewMgr := &ViewManager{
		tblMgr: tblMgr,
	}

	if isNew {
		sch := rm.NewSchema()
		sch.AddStringField("viewname", MAX_NAME)
		sch.AddStringField("viewdef", MAX_VIEWDEF)
		tblMgr.CreateTable("viewcat", sch, tx)
	}

	return viewMgr
}

func (v *ViewManager) CreateView(vname string, vdef string, tx *tx.Transaction) {
	//每创建一个视图对象，就在viewcat表中插入一条对该视图对象元数据的记录
	layout := v.tblMgr.GetLayout("viewcat", tx)
	table := rm.NewTableScan(tx, "viewcat", layout)
	table.Insert()
	table.SetString("viewname", vname)
	table.SetString("viewdef", vdef)
	table.Close()
}

func (v *ViewManager) GetViewDef(vname string, tx *tx.Transaction) string {
	res := ""
	layout := v.tblMgr.GetLayout("viewcat", tx)
	table := rm.NewTableScan(tx, "viewcat", layout)
	for table.Next() {
		if table.GetString("viewname") == vname {
			res = table.GetString("viewdef")
		}
	}

	table.Close()
	return res
}
