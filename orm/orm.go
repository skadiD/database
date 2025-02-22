package orm

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/skadiD/database"
)

type Orm[T any] struct {
	Client *database.Client
	Data   *T
	PkVal  any

	where []sq.Sqlizer
}

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func Model[T any](c *database.Client) *Orm[T] {
	var data T
	return &Orm[T]{Client: c, Data: &data}
}

func (m *Orm[T]) Load(data *T) *Orm[T] {
	m.Data = data
	return m
}

// Pk 设置主键值
func (m *Orm[T]) Pk(value any) *Orm[T] {
	m.PkVal = value
	return m
}

// Where 条件
func (m *Orm[T]) Where(where []sq.Sqlizer) *Orm[T] {
	m.where = where
	return m
}
