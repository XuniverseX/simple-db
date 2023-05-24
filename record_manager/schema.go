package record_manager

type FIELD_TYPE int

const (
	INTEGER FIELD_TYPE = iota
	VARCHAR
)

type FieldInfo struct {
	fieldType FIELD_TYPE
	length    int
}

func newFieldInfo(fieldType FIELD_TYPE, length int) *FieldInfo {
	return &FieldInfo{
		fieldType: fieldType,
		length:    length,
	}
}

type Schema struct {
	fields []string
	info   map[string]*FieldInfo
}

func NewSchema() *Schema {
	return &Schema{
		fields: make([]string, 0),
		info:   make(map[string]*FieldInfo),
	}
}

func (s *Schema) AddField(fieldName string, fieldType FIELD_TYPE, length int) {
	s.fields = append(s.fields, fieldName)
	s.info[fieldName] = newFieldInfo(fieldType, length)
}

func (s *Schema) AddIntField(fieldName string) {
	//整形字段的长度没有作用
	s.AddField(fieldName, INTEGER, 0)
}

func (s *Schema) AddStringField(fieldName string, length int) {
	s.AddField(fieldName, VARCHAR, length)
}

func (s *Schema) Add(fieldName string, sch SchemaInterface) {
	fieldType := sch.Type(fieldName)
	length := sch.Length(fieldName)
	s.AddField(fieldName, fieldType, length)
}

func (s *Schema) AddAll(sch SchemaInterface) {
	fields := sch.Fields()
	for _, val := range fields {
		s.Add(val, sch)
	}
}

func (s *Schema) Fields() []string {
	return s.fields
}

func (s *Schema) HasFields(fieldName string) bool {
	for _, field := range s.fields {
		if field == fieldName {
			return true
		}
	}
	return false
}

func (s *Schema) Type(fieldName string) FIELD_TYPE {
	return s.info[fieldName].fieldType
}

func (s *Schema) Length(fieldName string) int {
	return s.info[fieldName].length
}