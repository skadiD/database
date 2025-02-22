package orm

import (
	"reflect"
	"unsafe"

	"github.com/Masterminds/squirrel"
	"github.com/skadiD/database"
)

// Deleter 删除操作结构体
type Deleter struct {
	client *database.Client
	schema *database.TableSchema
	where  []squirrel.Sqlizer
}

// Delete 删除
func (m *Orm[T]) Delete() *Deleter {
	return m.buildDeleter()
}

// Where 额外条件
func (d *Deleter) Where(cond squirrel.Eq) *Deleter {
	d.where = append(d.where, cond)
	return d
}

// Run 执行删除
func (d *Deleter) Run() (int64, error) {
	query := psql.Delete(d.schema.TableName).Where(squirrel.And(d.where))
	return d.client.Delete(query)
}

func (m *Orm[T]) buildDeleter() *Deleter {
	schema := database.GetSchema(m.Data)
	deleter := &Deleter{
		client: m.Client,
		schema: schema,
		where:  m.where,
	}

	if schema.PrimaryKey != nil {
		var pkVal any
		if (m.PkVal == nil || database.IsZeroValue(m.PkVal)) && m.Data != nil {
			val := reflect.ValueOf(m.Data).Elem()
			ptr := val.UnsafeAddr()
			pkVal = *(*int64)(unsafe.Pointer(ptr + schema.PrimaryKey.Offset))
		} else {
			pkVal = m.PkVal
		}
		deleter.where = append(deleter.where, squirrel.Eq{schema.PrimaryKey.ColumnName: pkVal})
	}

	return deleter
}
