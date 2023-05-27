package metadata_manager

import (
	rm "record_manager"
	"tx"
)

type MetaDataManager struct {
	tblMgr  *TableManager
	viewMgr *ViewManager
	statMgr *StatManager
	//索引管理器以后再处理
	//idxMgr *IndexManager
}

func NewMetaDataManager(isNew bool, tx *tx.Transaction) *MetaDataManager {
	metaDataMgr := &MetaDataManager{
		tblMgr: NewTableManager(isNew, tx),
	}

	metaDataMgr.viewMgr = NewViewMananger(isNew, metaDataMgr.tblMgr, tx)
	metaDataMgr.statMgr = NewStatManager(metaDataMgr.tblMgr, tx)

	return metaDataMgr
}

func (m *MetaDataManager) CreateTable(tblName string, sch *rm.Schema, tx *tx.Transaction) {
	m.tblMgr.CreateTable(tblName, sch, tx)
}

func (m *MetaDataManager) CreateView(viewName string, viewDef string, tx *tx.Transaction) {
	m.viewMgr.CreateView(viewName, viewDef, tx)
}

func (m *MetaDataManager) GetLayout(tblName string, tx *tx.Transaction) *rm.Layout {
	return m.tblMgr.GetLayout(tblName, tx)
}

func (m *MetaDataManager) GetViewDef(viewName string, tx *tx.Transaction) string {
	return m.viewMgr.GetViewDef(viewName, tx)
}

func (m *MetaDataManager) GetStatInfo(tblName string, layout *rm.Layout, tx *tx.Transaction) *StatInfo {
	return m.statMgr.GetStatInfo(tblName, layout, tx)
}
