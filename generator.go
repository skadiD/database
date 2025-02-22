package database

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/fexli/logger"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var tbParams []string

var CustomFieldTypes = map[string]map[string]string{}

// 获取所有表名
func getTables(ctx context.Context) ([][]string, error) {
	rows, err := pool.Client.Query(ctx, `
SELECT 
    t.table_name,
    COALESCE(pgdesc.description, '') AS table_comment
FROM 
    information_schema.tables t
LEFT JOIN 
    pg_catalog.pg_class pgc 
    ON pgc.relname = t.table_name
    AND pgc.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public')
LEFT JOIN 
    pg_catalog.pg_description pgdesc 
    ON pgdesc.objoid = pgc.oid
    AND pgdesc.objsubid = 0  -- 表注释的 objsubid 为 0
LEFT JOIN 
    pg_inherits pi 
    ON pi.inhrelid = t.table_name::regclass
WHERE 
    t.table_schema = 'public'
    AND t.table_type = 'BASE TABLE'
    AND pi.inhrelid IS NULL
ORDER BY 
    t.table_name
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables [][]string
	for rows.Next() {
		var tableName, tableComment string
		if err := rows.Scan(&tableName, &tableComment); err != nil {
			return nil, err
		}
		if strings.HasPrefix(tableName, "pg_") || tableName == "spatial_ref_sys" || strings.HasPrefix(tableName, "geo") {
			continue
		}
		tables = append(tables, []string{tableName, tableComment})
	}

	return tables, nil
}

// 为指定表生成 Go struct
func generateStructForTable(ctx context.Context, table []string) (string, error) {
	tbParams = []string{}
	rows, err := pool.Client.Query(ctx, `
		SELECT 
			cols.column_name, 
			cols.udt_name, 
			COALESCE(pgdesc.description, '') AS column_comment,
			CASE WHEN cols.is_nullable = 'NO' THEN false ELSE true END AS is_nullable,
			CASE WHEN cols.is_identity = 'NO' THEN false ELSE true END AS is_identity,
			CASE 
				WHEN kcu.column_name IS NOT NULL THEN true 
				ELSE false 
			END AS is_primary_key
		FROM 
			information_schema.columns cols
		LEFT JOIN 
			pg_catalog.pg_description pgdesc 
			ON pgdesc.objsubid = cols.ordinal_position
			AND pgdesc.objoid = (
				SELECT pgc.oid 
				FROM pg_catalog.pg_class pgc 
				JOIN pg_catalog.pg_namespace pgns 
				ON pgc.relnamespace = pgns.oid 
				WHERE pgc.relname = cols.table_name
				AND pgns.nspname = 'public'
			)
		LEFT JOIN 
			information_schema.key_column_usage kcu 
			ON cols.table_name = kcu.table_name 
			AND cols.column_name = kcu.column_name 
			AND kcu.constraint_name IN (
				SELECT constraint_name 
				FROM information_schema.table_constraints 
				WHERE table_name = cols.table_name 
				AND constraint_type = 'PRIMARY KEY'
			)
		WHERE 
			cols.table_name = $1
	`, table[0])
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var structFields []string
	for rows.Next() {
		var columnName, dataType, columnComment string
		var isNil, isAuto, isPk bool
		if err := rows.Scan(&columnName, &dataType, &columnComment, &isNil, &isAuto, &isPk); err != nil {
			return "", err
		}
		jsonAttrs := []string{columnName}

		var customType string
		if tableFieldTypes, ok := CustomFieldTypes[table[0]]; ok {
			if fieldType, ok := tableFieldTypes[columnName]; ok {
				customType = fieldType
			}
		}

		var goType string
		if customType != "" {
			goType = customType
		} else {
			var isZeroNull bool
			goType, isZeroNull = pgTypeToGoType(columnName, dataType, isNil)

			if isNil {
				if isZeroNull {
					jsonAttrs = append(jsonAttrs, "omitempty")
				} else {
					goType = "*" + goType
				}
			}
		}

		if columnComment != "" { // 多行注释处理
			columnComment = "\t// " + strings.ReplaceAll(columnComment, "\n", "\n// ")
			if isNil {
				columnComment += " 【可空】"
			}
			structFields = append(structFields, columnComment)
		}

		// 判断是否自增和主键
		ormStr := "orm:\"" + columnName
		if isAuto {
			ormStr += ",auto"
		}
		if isPk {
			ormStr += ",pk"
		}
		ormStr += "\""
		fieldDecl := fmt.Sprintf("\t%s %s `db:\"%s\" json:\"%s\" %s`",
			toCamelCase(strings.Replace(columnName, ".", "_", 1)),
			goType,
			columnName,
			strings.Join(jsonAttrs, ","),
			ormStr,
		)
		structFields = append(structFields, fieldDecl)
		tbParams = append(tbParams, columnName)
	}

	structCode := fmt.Sprintf(
		`package db
import (
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
)
// %s %s
type %s struct {
%s
}
`, toCamelCase(table[0]), table[1], toCamelCase(table[0]), strings.Join(structFields, "\n"))
	return structCode, nil
}

// PostgreSQL 数据类型到 Go 数据类型的转换
func pgTypeToGoType(columnName, pgType string, nullable bool) (string, bool) {
	// 对可空情形进行特殊处理
	if nullable {
		switch pgType {
		case "integer", "int4":
			return "zeronull.Int4", true
		case "bigint", "int8":
			return "zeronull.Int8", true
		case "smallint", "int2":
			return "zeronull.Int2", true
		case "text", "varchar", "character varying":
			return "zeronull.Text", true
		case "timestamp", "timestamptz":
			return "types.ZeroNullJsonTime", true
		case "float4":
			return "zeronull.Float8", true
		}
		// 不属于对可空情形进行特殊处理的类型（如zeronull），继续下面的普通switch
	}

	switch pgType {
	case "integer", "int4":
		return "int", false
	case "bigint", "int8":
		return "int64", false
	case "smallint", "int2":
		return "int16", false
	case "boolean", "bool":
		return "bool", false
	case "text", "varchar", "character varying":
		return "string", false
	// case "timestamp without time zone", "timestamp with time zone", "timestamp":
	case "timestamp", "timestamptz":
		return "types.JsonTime", false
	// case "numeric", "decimal", "real", "float4":
	case "float4":
		return "float64", false
	case "_int4":
		return "[]int", false
	case "_varchar", "_text":
		return "[]string", false
	case "int4multirange":
		return "pgtype.Multirange[pgtype.Range[pgtype.Int4]]", false
	case "tsrange", "tstzrange":
		return "pgtype.Range[JsonTime]", false
	case "inet":
		return "netip.Addr", false
	case "hstore":
		return "pgtype.Hstore", false
	case "jsonb":
		return "ANY", false
	case "json":
		return "string", false
	case "geometry":
		return "GeometryPoint", false
	case "bytea":
		return "[]byte", false
	default:
		fmt.Println(columnName, "is unknown postgres type:", pgType)
		return "string", false
	}
}

func toCamelCase(s string) string {
	parts := strings.Split(s, "_")
	for i := 0; i < len(parts); i++ {
		parts[i] = cases.Title(language.English).String(parts[i])
	}
	return strings.Join(parts, "")
}

func writeStructToFile(fileName, structCode string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(structCode)
	return err
}

func (c *Client) ToStruct() {
	ctx := context.Background()
	tables, err := getTables(ctx)

	if err != nil {
		dbLog.Error(logger.WithContent("Error fetching tables", err))
		os.Exit(1)
	}
	var ts = map[string]string{}
	for _, table := range tables {
		tableDeclPath := fmt.Sprintf("db/model_%s_table.go", table[0])
		//modelDeclPath := fmt.Sprintf("db/model_%s.go", table)

		// 存储到 map 中
		ts[table[0]] = toCamelCase(table[0])

		structCode, err := generateStructForTable(ctx, table)
		if err != nil {
			dbLog.Error(logger.WithContent("Error generating struct for table ", table, err))
			continue
		}

		err = writeStructToFile(tableDeclPath, structCode)
		if err != nil {
			dbLog.Error(logger.WithContent("Error writing struct for table ", table, err))
			continue
		}

		err = runGoImports(tableDeclPath)
		if err != nil {
			dbLog.Error(logger.WithContent("Error formatting file: ", table, err))
			continue
		}

		dbLog.Notice(logger.WithContent("Struct for table", table, "written to file", tableDeclPath))

		//{
		//	_, err := os.Stat(modelDeclPath)
		//	if err == nil {
		//		continue
		//	}
		//
		//	template := map[string]string{
		//		"{{.TableName}}":    table,
		//		"{{.StructName}}":   toCamelCase(table),
		//		"{{.InsertCols}}":   getInsertColumns(),
		//		"{{.InsertParams}}": getInsertParams(),
		//		"{{.InsertVals}}":   getInsertValues(),
		//		"{{.UpdateCols}}":   getUpdateColumns(),
		//		"{{.UpdateVals}}":   getUpdateValues(),
		//	}
		//	content := readTemplateFile("template.tp", template)
		//
		//	err = writeStructToFile(modelDeclPath, content)
		//	if err != nil {
		//		dbLog.Error(logger.WithContent("Error writing model for table ", table, err))
		//		continue
		//	}
		//	go runGoImports(modelDeclPath)
		//
		//	//err = writeStructToFile(fmt.Sprintf("db/model_%s_test.go", table), "package db\n\n")
		//	//if err != nil {
		//	//	dbLog.Error(logger.WithContent("Error writing model test for table ", table, err))
		//	//	continue
		//	//}
		//}
	}
	fmt.Println(ts)
}

func runGoImports(fileName string) error {
	cmd := exec.Command("goimports", "-w", fileName)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to run goimports: %v", err)
	}
	return nil
}

func readTemplateFile(tempName string, params map[string]string) string {
	file, err := os.Open(tempName)
	if err != nil {
		return ""
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return ""
	}

	for k, v := range params {
		content = bytes.ReplaceAll(content, []byte(k), []byte(v))
	}
	return string(content)
}

// TODO
func getInsertColumns() string {
	return ""
}
func getUpdateValues() string {
	return ""
}
func getUpdateColumns() string {
	return ""
}
func getInsertValues() string {
	return ""
}
func getInsertParams() string {
	return ""
}
