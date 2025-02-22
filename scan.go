package database

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func RowToStructByName[T any](row pgx.CollectableRow) (T, error) {
	var value T
	err := (&namedStructRowScanner{ptrToStruct: &value}).ScanRow(row)
	return value, err
}

type namedStructRowScanner struct {
	ptrToStruct any
	lax         bool
}

func (rs *namedStructRowScanner) ScanRow(rows pgx.CollectableRow) error {
	typ := reflect.TypeOf(rs.ptrToStruct).Elem()
	// 返回字段
	fldDescs := rows.FieldDescriptions()
	namedStructFields, err := lookupNamedStructFields(typ, fldDescs)
	if err != nil {
		return err
	}
	if !rs.lax && namedStructFields.missingField != "" {
		return fmt.Errorf("cannot find field %s in returned row", namedStructFields.missingField)
	}
	fields := namedStructFields.fields
	scanTargets := setupStructScanTargets(rs.ptrToStruct, fields)
	return rows.Scan(scanTargets...)
}

type structRowField struct {
	path []int
}

type namedStructFields struct {
	fields []structRowField
	// missingField is the first field from the struct without a corresponding row field.
	// This is used to construct the correct error message for non-lax queries.
	missingField string
}

type namedStructFieldsKey struct {
	t        reflect.Type
	colNames string
}

var namedStructFieldMap sync.Map

const structTagKey = "db"

func lookupNamedStructFields(t reflect.Type, fldDescs []pgconn.FieldDescription) (*namedStructFields, error) {
	key := namedStructFieldsKey{
		t:        t,
		colNames: joinFieldNames(fldDescs),
	}
	if cached, ok := namedStructFieldMap.Load(key); ok {
		return cached.(*namedStructFields), nil
	}

	// We could probably do two-levels of caching, where we compute the key -> fields mapping
	// for a type only once, cache it by type, then use that to compute the column -> fields
	// mapping for a given set of columns.
	fieldStack := make([]int, 0, 1)
	fields, missingField := computeNamedStructFields(
		fldDescs,
		t,
		make([]structRowField, len(fldDescs)),
		&fieldStack, "",
	)
	for i, f := range fields {
		if f.path == nil {
			return nil, fmt.Errorf(
				"struct doesn't have corresponding row field %s",
				fldDescs[i].Name,
			)
		}
	}

	fieldsIface, _ := namedStructFieldMap.LoadOrStore(
		key,
		&namedStructFields{fields: fields, missingField: missingField},
	)
	return fieldsIface.(*namedStructFields), nil
}

func computeNamedStructFields(
	fldDescs []pgconn.FieldDescription,
	t reflect.Type,
	fields []structRowField,
	fieldStack *[]int,
	prefix string,
) ([]structRowField, string) {
	var missingField string
	tail := len(*fieldStack)
	*fieldStack = append(*fieldStack, 0)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		(*fieldStack)[tail] = i
		if sf.PkgPath != "" && !sf.Anonymous {
			// Field is unexported, skip it.
			continue
		}
		// Handle anonymous struct embedding, but do not try to handle embedded pointers.
		dbTag := sf.Tag.Get(structTagKey)
		// if dbTagPresent {
		// 	dbTag, _, _ = strings.Cut(dbTag, ",")
		// }
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			var prefixStr string
			if dbTag != "" {
				prefixStr = prefix + dbTag + "_"
			}
			var missingSubField string

			fields, missingSubField = computeNamedStructFields(
				fldDescs,
				sf.Type,
				fields,
				fieldStack,
				prefixStr,
			)
			if missingField == "" {
				missingField = missingSubField
			}
		} else {
			if dbTag == "-" {
				// Field is ignored, skip it.
				continue
			}
			colName := prefix + dbTag
			// fpos := fieldPosByName(fldDescs, colName, !dbTagPresent)
			// 可删除 dbTagPresent 是因为 db_gen 生成的字段均含有 `db` tag
			fpos := fieldPosByName(fldDescs, colName)
			if fpos == -1 {
				if missingField == "" {
					missingField = colName
				}
				continue
			}
			fields[fpos] = structRowField{
				path: append([]int(nil), *fieldStack...),
			}
		}
	}
	*fieldStack = (*fieldStack)[:tail]

	return fields, missingField
}
func joinFieldNames(fldDescs []pgconn.FieldDescription) string {
	switch len(fldDescs) {
	case 0:
		return ""
	case 1:
		return fldDescs[0].Name
	}

	totalSize := len(fldDescs) - 1 // Space for separator bytes.
	for _, d := range fldDescs {
		totalSize += len(d.Name)
	}
	var b strings.Builder
	b.Grow(totalSize)
	b.WriteString(fldDescs[0].Name)
	for _, d := range fldDescs[1:] {
		b.WriteByte(0) // Join with NUL byte as it's (presumably) not a valid column character.
		b.WriteString(d.Name)
	}

	return b.String()
}

func fieldPosByName(fldDescs []pgconn.FieldDescription, field string) (i int) {
	i = -1
	for i, desc := range fldDescs {
		if desc.Name == field {
			// fmt.Println("->search idx:", i)
			return i
		}
	}
	return
}

func setupStructScanTargets(receiver any, fields []structRowField) []any {
	scanTargets := make([]any, len(fields))
	v := reflect.ValueOf(receiver).Elem()
	for i, f := range fields {
		scanTargets[i] = v.FieldByIndex(f.path).Addr().Interface()
	}
	return scanTargets
}
