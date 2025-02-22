package custom_row2struct

import (
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"reflect"
	"strings"
	"sync"
)

// pgx.RowToStructByName魔改版

type CustomScanFromRow struct {
	RowField string
	// 扫描值接收器位置
	Ord int
}

// CustomScanField 表示结构外的自定义扫描目标
type CustomScanField struct {
	// 扫描目标在DB返回的row中的列索引，所有自定义扫描目标的插入位置将会作为最终 fields []structRowField 位置的排除项
	Pos int
	// 扫描值接收器位置
	Ord int
}

func SetupCustomStructScanTargets(receiver any, nsf *NamedStructFieldsWithCustomTarget, customTargetVals ...any) ([]any, error) {
	fields := nsf.fields
	customTargets := nsf.customFields

	if len(customTargets) != len(customTargetVals) {
		return nil, fmt.Errorf("custom target receiver count mismatch: expected %d, got %d",
			len(customTargets), len(customTargetVals))
	}

	scanTargets := make([]any, len(fields))
	for _, c := range customTargets {
		if c.Pos < 0 || c.Pos >= len(scanTargets) {
			return nil, fmt.Errorf("invalid custom target position: %d (must be between 0 and %d)", c.Pos, len(scanTargets)-1)
		}
		if scanTargets[c.Pos] != nil {
			return nil, fmt.Errorf("duplicate custom target position: %d", c.Pos)
		}
		if c.Ord >= len(customTargetVals) {
			return nil, fmt.Errorf("invalid custom target receiver at ord: %d (must be between 0 and %d)", c.Ord, len(customTargetVals)-1)
		}
		if customTargetVals[c.Ord] == nil {
			return nil, fmt.Errorf("invalid custom target receiver at ord: %d (cannot be nil)", c.Ord)
		}
		scanTargets[c.Pos] = customTargetVals[c.Ord]
	}

	v := reflect.ValueOf(receiver).Elem()
	for i, f := range fields {
		if scanTargets[i] == nil {
			scanTargets[i] = v.FieldByIndex(f.path).Addr().Interface()
		}
	}

	return scanTargets, nil
}

// Map from namedStructFieldMap -> *NamedStructFieldsWithCustomTarget
var namedStructFieldMap sync.Map

type namedStructFieldsKey struct {
	t                 reflect.Type
	customScanTargets string
	colNames          string
}

type NamedStructFieldsWithCustomTarget struct {
	fields       []structRowField
	customFields []CustomScanField
	// missingField is the first field from the struct without a corresponding row field.
	// This is used to construct the correct error message for non-lax queries.
	missingField string
}

func (n *NamedStructFieldsWithCustomTarget) RequireStructFieldsInRows() error {
	if n.missingField != "" {
		return fmt.Errorf("cannot find field %s in returned row", n.missingField)
	}
	return nil
}

func LookupNamedStructFields(
	fldDescs []pgconn.FieldDescription,
	t reflect.Type,
	customScanTargets []CustomScanFromRow,
) (*NamedStructFieldsWithCustomTarget, error) {
	key := namedStructFieldsKey{
		t:                 t,
		customScanTargets: joinCustomScanTargetNames(customScanTargets),
		colNames:          joinFieldNames(fldDescs),
	}
	if cached, ok := namedStructFieldMap.Load(key); ok {
		return cached.(*NamedStructFieldsWithCustomTarget), nil
	}

	// We could probably do two-levels of caching, where we compute the key -> fields mapping
	// for a type only once, cache it by type, then use that to compute the column -> fields
	// mapping for a given set of columns.
	fieldStack := make([]int, 0, 1)
	fields, missingField := computeNamedStructFields(
		fldDescs,
		t,
		make([]structRowField, len(fldDescs)),
		&fieldStack,
	)

	var customScanFields []CustomScanField
	for i, f := range fields {
		if f.path != nil {
			continue
		}

		found := false
		for _, c := range customScanTargets {
			if c.RowField == fldDescs[i].Name {
				customScanFields = append(customScanFields, CustomScanField{
					Pos: i,
					Ord: c.Ord,
				})
				found = true
				break
			}

		}
		if !found {
			return nil, fmt.Errorf(
				"row field '%s' does not match any scan targets",
				fldDescs[i].Name,
			)
		}
	}

	if len(customScanFields) != len(customScanTargets) {
		for _, cst := range customScanTargets {
			found := false
			for _, csf := range customScanFields {
				if csf.Ord == cst.Ord {
					found = true
					break
				}
			}

			if !found {
				return nil, fmt.Errorf(
					"custom scan target with row field '%s' does not have a corresponding custom scan field",
					cst.RowField,
				)
			}
		}
	}

	fieldsIface, _ := namedStructFieldMap.LoadOrStore(
		key,
		&NamedStructFieldsWithCustomTarget{
			fields:       fields,
			customFields: customScanFields,
			missingField: missingField,
		},
	)
	return fieldsIface.(*NamedStructFieldsWithCustomTarget), nil
}

func joinCustomScanTargetNames(customTargets []CustomScanFromRow) string {
	var b strings.Builder
	totalSize := len(customTargets)*2 - 1
	for _, t := range customTargets {
		totalSize += len(t.RowField)
		if t.Ord < 10 {
			totalSize += 1
		} else if t.Ord < 100 {
			totalSize += 2
		} else {
			for pos := t.Ord; pos > 0; pos /= 10 {
				totalSize++
			}
		}
	}
	b.Grow(totalSize)
	b.WriteString(customTargets[0].RowField)
	b.WriteByte(0)
	b.WriteString(fmt.Sprint(customTargets[0].Ord))
	for _, t := range customTargets[1:] {
		b.WriteByte(0)
		b.WriteString(t.RowField)
		b.WriteByte(0)
		b.WriteString(fmt.Sprint(t.Ord))
	}
	return b.String()
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

func computeNamedStructFields(
	fldDescs []pgconn.FieldDescription,
	t reflect.Type,
	fields []structRowField,
	fieldStack *[]int,
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
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			var missingSubField string
			fields, missingSubField = computeNamedStructFields(
				fldDescs,
				sf.Type,
				fields,
				fieldStack,
			)
			if missingField == "" {
				missingField = missingSubField
			}
		} else {
			dbTag, dbTagPresent := sf.Tag.Lookup(structTagKey)
			if dbTagPresent {
				dbTag, _, _ = strings.Cut(dbTag, ",")
			}
			if dbTag == "-" {
				// Field is ignored, skip it.
				continue
			}
			colName := dbTag
			if !dbTagPresent {
				colName = sf.Name
			}
			fpos := fieldPosByName(fldDescs, colName, !dbTagPresent)
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

const structTagKey = "db"

func fieldPosByName(fldDescs []pgconn.FieldDescription, field string, normalize bool) (i int) {
	i = -1

	if normalize {
		field = strings.ReplaceAll(field, "_", "")
	}
	for i, desc := range fldDescs {
		if normalize {
			if strings.EqualFold(strings.ReplaceAll(desc.Name, "_", ""), field) {
				return i
			}
		} else {
			if desc.Name == field {
				return i
			}
		}
	}
	return
}

// structRowField describes a field of a struct.
type structRowField struct {
	path []int
}
