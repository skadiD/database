package database

import (
	"reflect"
	"strings"
	"unsafe"
)

// FieldSchema 字段元数据
type FieldSchema struct {
	GoName     string       // Go字段名
	ColumnName string       // 数据库列名
	Offset     uintptr      // 字段偏移量
	GoType     reflect.Type // Go类型
	PrimaryKey bool         // 是否主键
	AutoIncr   bool         // 是否自增
}

// TableSchema 表元数据
type TableSchema struct {
	GoType        reflect.Type
	TableName     string
	Fields        []*FieldSchema
	PrimaryKey    *FieldSchema
	ColumnToField map[string]*FieldSchema
}

// 空接口实际上是具有两个指针的结构的语法糖：第一个指向有关类型的信息，第二个指向值
// 可以使用结构体中字段偏移量来直接寻址该值的字段
type interfaceMark struct {
	typ   unsafe.Pointer
	value unsafe.Pointer
}

var (
	schemaCache = make(map[uintptr]*TableSchema) // 表缓存
)

// RegisterModel 注册表模型
//
// Warning: 线程不安全
func RegisterModel[T any](tableName string) error {
	var model T
	typ := reflect.TypeOf(model)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	// 检查是否已注册
	infMark := (*interfaceMark)(unsafe.Pointer(&typ))
	//fmt.Println("cache", typ.String(), infMark.value)
	if _, ok := schemaCache[uintptr(infMark.value)]; ok {
		return nil
	}

	schema := &TableSchema{
		GoType:        typ,
		TableName:     "\"" + tableName + "\"",
		ColumnToField: make(map[string]*FieldSchema),
	}

	numField := typ.NumField()
	for i := 0; i < numField; i++ {
		field := typ.Field(i)

		// 跳过非导出字段
		if field.PkgPath != "" {
			continue
		}

		// 解析字段标签
		tag := field.Tag.Get("orm")
		if tag == "-" {
			continue
		}

		fieldSchema := parseFieldSchema(field, tag)
		schema.Fields = append(schema.Fields, fieldSchema)
		schema.ColumnToField[fieldSchema.ColumnName] = fieldSchema

		// 记录主键
		if fieldSchema.PrimaryKey {
			schema.PrimaryKey = fieldSchema
		}
	}

	schemaCache[uintptr(infMark.value)] = schema
	return nil
}

// parseFieldSchema 解析字段标签
func parseFieldSchema(field reflect.StructField, tag string) *FieldSchema {
	parts := strings.Split(tag, ",")
	fieldSchema := &FieldSchema{
		GoName:     field.Name,
		ColumnName: field.Name, // 默认使用字段名
		Offset:     field.Offset,
		GoType:     field.Type,
	}

	if len(parts) > 0 && parts[0] != "" {
		fieldSchema.ColumnName = parts[0]
	}

	for _, opt := range parts[1:] {
		switch opt {
		case "pk":
			fieldSchema.PrimaryKey = true
		case "auto":
			fieldSchema.AutoIncr = true
		}
	}

	return fieldSchema
}

// GetSchema 获取表元数据
func GetSchema(model any) *TableSchema {
	typ := reflect.TypeOf(model)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	infMark := (*interfaceMark)(unsafe.Pointer(&typ))
	//fmt.Println("load", typ.String(), infMark.value)
	if schema, ok := schemaCache[uintptr(infMark.value)]; ok {
		return schema
	}
	panic("model not registered: " + typ.String())
}
