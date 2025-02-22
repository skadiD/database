package orm

import (
	"github.com/Masterminds/squirrel"
	"github.com/skadiD/database"
)

// Selector 查询操作结构体
type Selector[T any] struct {
	client  *database.Client
	schema  *database.TableSchema
	columns []string
	where   []squirrel.Sqlizer
	orderBy []string
	limit   uint64
	offset  uint64
	joins   []string
}

// Select 初始化查询
func (m *Orm[T]) Select(cols ...string) *Selector[T] {
	schema := database.GetSchema(m.Data)
	selector := &Selector[T]{
		client:  m.Client,
		schema:  schema,
		columns: cols,
		where:   m.where,
	}

	// 默认选择所有字段
	if len(cols) == 0 {
		for _, field := range schema.Fields {
			selector.columns = append(selector.columns, field.ColumnName)
		}
	}

	// 增加快速主键查询
	if schema.PrimaryKey != nil && !database.IsZeroValue(m.PkVal) {
		selector.where = append(selector.where, squirrel.Eq{schema.PrimaryKey.ColumnName: m.PkVal})
	}

	return selector
}

// Where 添加条件
func (s *Selector[T]) Where(cond squirrel.Sqlizer) *Selector[T] {
	s.where = append(s.where, cond)
	return s
}

// OrderBy 添加排序
func (s *Selector[T]) OrderBy(clauses ...string) *Selector[T] {
	s.orderBy = append(s.orderBy, clauses...)
	return s
}

// Limit 设置限制
func (s *Selector[T]) Limit(limit uint64) *Selector[T] {
	s.limit = limit
	return s
}

// Page 分页
func (s *Selector[T]) Page(page, size uint64) *Selector[T] {
	s.limit = size
	s.offset = (page - 1) * size
	return s
}

// Offset 设置偏移
func (s *Selector[T]) Offset(offset uint64) *Selector[T] {
	s.offset = offset
	return s
}

// Join 添加连接
func (s *Selector[T]) Join(join string) *Selector[T] {
	s.joins = append(s.joins, join)
	return s
}

// Get 多条查询
func (s *Selector[T]) Get() ([]T, error) {
	return database.Select[T](s.client.Client, s.sql())
}

// One 获取单条记录
func (s *Selector[T]) One() (*T, error) {
	query := s.sql().Limit(1)
	return database.Get[T](s.client.Client, query)
}

func (s *Selector[T]) sql() squirrel.SelectBuilder {
	query := psql.Select(s.columns...).
		From(s.schema.TableName)

	for _, join := range s.joins {
		query = query.Join(join)
	}

	if len(s.where) > 0 {
		query = query.Where(squirrel.And(s.where))
	}

	if len(s.orderBy) > 0 {
		query = query.OrderBy(s.orderBy...)
	}

	if s.limit > 0 {
		query = query.Limit(s.limit)
	}
	if s.offset > 0 {
		query = query.Offset(s.offset)
	}

	return query
}
