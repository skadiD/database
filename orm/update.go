package orm

import (
	"reflect"
	"unsafe"

	"github.com/Masterminds/squirrel"
	"github.com/skadiD/database"
)

// Updater 更新操作结构体
type Updater struct {
	client *database.Client
	schema *database.TableSchema
	values map[string]any
	where  []squirrel.Sqlizer
}

// Update 更新
func (m *Orm[T]) Update() (int64, error) {
	return m.buildUpdater(true).Run()
}

// Save 保存
func (m *Orm[T]) Save() (int64, error) {
	return m.buildUpdater(false).Run()
}

// Updates 更新
func (m *Orm[T]) Updates(cols map[string]any) (int64, error) {
	schema := database.GetSchema(m.Data)
	updater := &Updater{
		client: m.Client,
		schema: schema,
		values: cols,
		where:  m.where,
	}

	// 仅支持使用 m.Pk() 设置主键
	if schema.PrimaryKey != nil && m.PkVal != nil {
		updater.where = append(updater.where, squirrel.Eq{schema.PrimaryKey.ColumnName: m.PkVal})
	}

	return updater.Run()
}

// Where 额外条件
func (u *Updater) Where(cond squirrel.Sqlizer) *Updater {
	u.where = append(u.where, cond)
	return u
}

// Run 执行更新
func (u *Updater) Run() (int64, error) {
	query := psql.Update(u.schema.TableName).SetMap(u.values).Where(squirrel.And(u.where))
	return u.client.Update(query)
}

func (m *Orm[T]) buildUpdater(skipZero bool) *Updater {
	schema := database.GetSchema(m.Data)
	updater := &Updater{
		client: m.Client,
		schema: schema,
		values: make(map[string]any),
	}

	val := reflect.ValueOf(m.Data).Elem()
	ptr := val.UnsafeAddr()

	for _, field := range schema.Fields {
		if field.PrimaryKey {
			continue
		}

		fieldVal := reflect.NewAt(field.GoType, unsafe.Pointer(ptr+field.Offset)).Elem().Interface()
		if skipZero && database.IsZeroValue(fieldVal) {
			continue
		}
		updater.values[field.ColumnName] = fieldVal
	}

	if schema.PrimaryKey != nil {
		var pkVal any
		if m.PkVal == nil || database.IsZeroValue(m.PkVal) {
			pkVal = *(*int64)(unsafe.Pointer(ptr + schema.PrimaryKey.Offset))
		} else {
			pkVal = m.PkVal
		}
		updater.where = append(updater.where, squirrel.Eq{schema.PrimaryKey.ColumnName: pkVal})
	}

	return updater
}
