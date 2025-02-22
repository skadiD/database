package orm

import (
	"reflect"
	"unsafe"

	"github.com/skadiD/database"
)

// Inserter 插入操作结构体
type Inserter struct {
	client *database.Client
	schema *database.TableSchema
	values map[string]any
}

// Create 创建单条记录
func (m *Orm[T]) Create() (int64, error) {
	return m.buildInserter().Run()
}

// Run 执行插入
//
// TODO: 实现 RETURNING 语法
func (i *Inserter) Run() (int64, error) {
	query := psql.Insert(i.schema.TableName).SetMap(i.values)
	return i.client.Insert(query)
}

func (m *Orm[T]) buildInserter() *Inserter {
	schema := database.GetSchema(m.Data)
	inserter := &Inserter{
		client: m.Client,
		schema: schema,
		values: make(map[string]any),
	}

	val := reflect.ValueOf(m.Data).Elem()
	ptr := val.UnsafeAddr()

	for _, field := range schema.Fields {
		if field.PrimaryKey || field.AutoIncr {
			continue
		}

		fieldVal := reflect.NewAt(field.GoType, unsafe.Pointer(ptr+field.Offset)).Elem().Interface()
		inserter.values[field.ColumnName] = fieldVal
	}

	return inserter
}
