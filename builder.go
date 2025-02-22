package database

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/skadiD/database/scan/custom_row2struct"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/lann/builder"
	"github.com/modern-go/reflect2"
)

var (
	psql             = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	StatementBuilder = psql
	DefaultHook      = func(b sq.SelectBuilder) sq.SelectBuilder {
		return b
	}
)

const (
	MinPageElements = 10
)

func struct2name(name string) string {
	name = strings.Split(name, ".")[1]
	var result []rune
	for i, r := range name {
		if unicode.IsUpper(r) {
			if i > 0 {
				result = append(result, '_')
			}
			result = append(result, unicode.ToLower(r))
		} else {
			result = append(result, r)
		}
	}
	return string(result)
}

func Select[T any](db pgxscan.Querier, sb sq.SelectBuilder) ([]T, error) {
	sql, args, err := sb.ToSql()
	if err != nil {
		execErr(errors.Join(err, errors.New("error building SQL")), "", "database.Select")
		return nil, err
	}
	var results []T
	err = pgxscan.Select(context.Background(), db, &results, sql, args...)
	if err != nil {
		err = errors.Join(err,
			fmt.Errorf("error executing SQL:\n#### SQL:\n%s\n#### Args:\n%v", sql, args))
		execErr(err, "", "database.Select")
	}
	return results, err
}

func Get[T any](db pgxscan.Querier, sb sq.SelectBuilder) (*T, error) {
	sql, args, err := sb.ToSql()
	if err != nil {
		execErr(errors.Join(err, errors.New("error building SQL")), "", "database.Get")
		return nil, err
	}
	var result T
	err = pgxscan.Get(context.Background(), db, &result, sql, args...)
	if err != nil {
		err = errors.Join(err,
			fmt.Errorf("error executing SQL:\n#### SQL:\n%s\n#### Args:\n%v", sql, args))
		execErr(err, "", "database.Get")
	}
	return &result, err
}

// GetAll 获取某表全部数据 通过 hook 钩子函数进行拓展
//
// 文档地址 https://github.com/Masterminds/squirrel
func GetAll[T any](table string, page, size uint64, hook func(sq.SelectBuilder) sq.SelectBuilder) ([]T, int) {
	page, size = sanitizePageAndSize(page, size)
	var count int
	sb := hook(psql.Select("COUNT(*)").From(table))
	sql, args, _ := builder.Delete(sb, "OrderByParts").(sq.SelectBuilder).ToSql()

	err := pool.Client.QueryRow(context.Background(), sql, args...).Scan(&count)
	execErr(err, table, "GetAll - Count")

	b := hook(psql.Select("*").From(table))
	sql, args, _ = b.Limit(size).Offset((page - 1) * size).ToSql()
	row, _ := pool.Client.Query(context.Background(), sql, args...)
	urs, err := pgx.CollectRows(row, RowToStructByName[T])
	execErr(err, table, "GetAll - Pagination")
	return urs, count
}

func GetAllByFields[T any](table string, fields []string, sort []string, page, size uint64, hook func(sq.SelectBuilder) sq.SelectBuilder) ([]T, int) {
	page, size = sanitizePageAndSize(page, size)
	var count int

	// 使用传入的字段列表构建 COUNT 查询
	sql, args, _ := hook(psql.Select("COUNT(*)").From(table)).ToSql()
	err := pool.Client.QueryRow(context.Background(), sql, args...).Scan(&count)
	execErr(err, table, "GetAll - Count")

	// 使用传入的字段列表构建 SELECT 查询
	b := hook(psql.Select(fields...).From(table))
	sql, args, _ = b.Limit(size).Offset((page - 1) * size).OrderBy(sort...).ToSql()
	row, err := pool.Client.Query(context.Background(), sql, args...)
	urs, err := pgx.CollectRows(row, RowToStructByName[T])
	execErr(err, table, "GetAll - Pagination")
	return urs, count
}

// GetAllByFieldsCte 使用CTE实现将三个查询合并到同一语句来执行需要对主表JOIN和LIMIT的分页查询，
// 同时返回主表查询结果和主记录总数（用于分页）。
// 注意：当主表需要在JOIN的同时通过COUNT(*)来计算主记录的行数或LIMIT来限制主记录的行数（比如分页），
// 请使用此方法避免COUNT(*)结果偏大（主记录因为JOIN重复），或者JOIN得到的从表记录返回不完整（被LIMIT截断）。
//
// -- 第一个CTE 预选全部ID（GROUP BY用于在预选条件中存在JOIN的情况下去重）:
//
// WITH __cte_all_ids AS (SELECT <id> FROM <主表> <预选条件>... GROUP BY <id> <ORDER>...),
//
// -- 第二个CTE 计算总ID数:
//
// __cte_count AS (SELECT COUNT(*) as pagination_total FROM cte_all_ids)
//
// -- 实际查询:
// SELECT <主表字段>..., __cte_count.pagination_total
// FROM <主表>
// JOIN (SELECT <id> FROM __cte_all_ids <LIMIT> <OFFSET>) as __cts_ids ON __cte_ids.<id> = <主表>.<id>
// CROSS JOIN __cte_count
func GetAllByFieldsCte[T any](
	// 数据库连接对象
	db pgxscan.Querier,
	// 主表名或表名+别名，如：trader或"trader t"
	fromTable,
	// 主表ID列名
	idField string,
	// 排序字段，如：[]string{"id DESC"}
	sort []string,
	// 分页页码，从1开始
	page,
	// 每页大小
	size uint64,
	// buildPreselect 预选查询构建函数，应当在预选查询中完成条件筛选
	buildPreselect func(selectIdFrom sq.SelectBuilder) sq.SelectBuilder,
	// buildPrimary 主查询构建函数，不应当在主查询中完成条件筛选
	buildPrimary func(selectPrimaryFrom sq.SelectBuilder) sq.SelectBuilder,
) ([]T, int, bool) {
	// 没有获取到任何记录时返回默认总数 (page - 1) * size
	// 实际上可以用RIGHT JOIN返回一个[总数=n 主记录=NULL]的行来获取总数，但是这样要额外处理主记录=NULL的情况
	count := (page - 1) * size
	page, size = sanitizePageAndSize(page, size)

	countField := "__pagination_count"
	cteAllIds := "__cte_all_ids"
	cteCount := "__cte_count"
	_, tableRef := parseFromTableExpr(fromTable)

	idFieldPath := fmt.Sprintf(`"%s"."%s"`, tableRef, idField)
	cteCountPath := fmt.Sprintf(`"%s"."%s"`, cteCount, countField)

	var rn string
	if len(sort) > 0 {
		rn = fmt.Sprintf(`ROW_NUMBER() OVER (ORDER BY %s) AS "__rn"`, strings.Join(sort, " "))
	} else {
		rn = `ROW_NUMBER() OVER () AS "__rn"`
	}

	selectAllIdsSb := buildPreselect(psql.Select(idFieldPath, rn)).
		From(fromTable).
		GroupBy(idFieldPath)

	countIdOuterSb := psql.
		Select(`COUNT(*) AS "` + countField + `"`).
		From(cteAllIds)
	// JOIN __cte_all_ids 到主表带LIMIT子查询
	joinCteIdToPrimary := psql.
		Select(idField, "__rn").
		From(cteAllIds).
		OrderBy("__rn").
		Limit(size).
		Offset((page - 1) * size).
		Prefix("JOIN (").
		Suffix(fmt.Sprintf(`) AS __ids ON __ids."%s" = "%s"."%s"`, idField, tableRef, idField))
	joinCteCount := fmt.Sprintf(`"%s"`, cteCount)

	cte := WithCte(NewCte(cteAllIds, selectAllIdsSb), NewCte(cteCount, countIdOuterSb))
	primarySb := buildPrimary(
		psql.
			Select(cteCountPath).
			From(fromTable).
			JoinClause(joinCteIdToPrimary),
	).
		PrefixExpr(cte).
		CrossJoin(joinCteCount).
		OrderBy(`__ids.__rn`)

	sql, args, err := primarySb.ToSql()
	if err != nil {
		execErr(errors.Join(err, errors.New("error building SQL")), "", "database.GetAllByFieldsCte")
		return nil, 0, false
	}
	row, err := db.Query(context.Background(), sql, args...)
	if err != nil {
		sqlErr := fmt.Errorf("error executing db.Query:\n#### SQL:\n%s\n#### Args:\n%v", sql, args)
		execErr(errors.Join(err, sqlErr), "", "database.GetAllByFieldsCte")
	}

	var typ reflect.Type
	{
		var value *T
		typ = reflect.TypeOf(value).Elem()
	}

	customScanTargets := []custom_row2struct.CustomScanFromRow{
		{
			RowField: countField,
			Ord:      0,
		},
	}
	res, err := pgx.CollectRows(row, func(row pgx.CollectableRow) (T, error) {
		var value T
		fldDescs := row.FieldDescriptions()
		nsf, err := custom_row2struct.LookupNamedStructFields(fldDescs, typ, customScanTargets)
		if err != nil {
			return value, err
		}
		if err = nsf.RequireStructFieldsInRows(); err != nil {
			return value, err
		}
		scanTargets, err := custom_row2struct.SetupCustomStructScanTargets(&value, nsf, &count)
		if err != nil {
			return value, err
		}
		if err := row.Scan(scanTargets...); err != nil {
			return value, err
		}
		return value, nil
	})

	return res, int(count), execErr(err, "", "database.GetAllByFieldsCte")
}

// GetOneFromStructNameTable 获取某表一条数据 通过 hook 钩子函数进行拓展
//
// 文档地址 https://github.com/Masterminds/squirrel
func GetOneFromStructNameTable[T any](cols []string, hook func(sq.SelectBuilder) sq.SelectBuilder) (T, bool) {
	var data T
	tableName := struct2name(reflect2.TypeOf(data).String())
	b := hook(psql.Select(cols...).From(tableName))
	sql, args, _ := b.ToSql()

	row, _ := pool.Client.Query(context.Background(), sql, args...)
	urs, err := pgx.CollectOneRow(row, RowToStructByName[T])
	if !execErr(err, tableName, "GetOne") {
		return urs, false
	}
	return urs, true
}

// GetOne 获取某表一条数据 通过 hook 钩子函数进行拓展
//
// 使用本机 RowToStructByName，不支持联立 select * row2struct
//
// 文档地址 https://github.com/Masterminds/squirrel
func GetOne[T any](tableName string, cols []string, hook func(sq.SelectBuilder) sq.SelectBuilder) (T, bool) {
	b := hook(psql.Select(cols...).From(tableName))
	sql, args, _ := b.ToSql()

	row, _ := pool.Client.Query(context.Background(), sql, args...)
	urs, err := pgx.CollectOneRow(row, RowToStructByName[T])
	if !execErr(err, tableName, "GetOne") {
		return urs, false
	}
	return urs, true
}

// GetOne2 获取某表一条数据 通过 hook 钩子函数进行拓展
//
// 使用 pgx.RowToStructByName，支持联立 select * row2struct
//
// 文档地址 https://github.com/Masterminds/squirrel
func GetOne2[T any](tableName string, cols []string, hook func(sq.SelectBuilder) sq.SelectBuilder) (T, bool) {
	b := hook(psql.Select(cols...).From(tableName))
	sql, args, _ := b.ToSql()

	row, _ := pool.Client.Query(context.Background(), sql, args...)
	urs, err := pgx.CollectOneRow(row, pgx.RowToStructByName[T])
	if !execErr(err, tableName, "GetOne2") {
		return urs, false
	}
	return urs, true
}

// GetCount 获取某表数据条数
//
// 文档地址 https://github.com/Masterminds/squirrel
func GetCount(table string, hook func(sq.SelectBuilder) sq.SelectBuilder) int {
	var count int
	sql, args, _ := hook(psql.Select("COUNT(*)").From(table)).ToSql()
	err := pool.Client.QueryRow(context.Background(), sql, args...).Scan(&count)
	execErr(err, table, "GetCount")
	return count
}

// Insert 为某表添加记录 返回是否成功
//
// 文档地址 https://github.com/Masterminds/squirrel
func Insert[T any](hook func(sq.InsertBuilder) sq.InsertBuilder) (T, bool) {
	var data T
	tableName := struct2name(reflect2.TypeOf(data).String())
	b := hook(psql.Insert(tableName)).Suffix("RETURNING *")
	sql, args, _ := b.ToSql()
	row, err := pool.Client.Query(context.Background(), sql, args...)
	ret, err := pgx.CollectOneRow(row, RowToStructByName[T])
	return ret, execErr(err, tableName, "Insert")
}

// Update 为某表更新记录 返回是否成功
//
// 文档地址 https://github.com/Masterminds/squirrel
func Update[T any](hook func(sq.UpdateBuilder) sq.UpdateBuilder) (T, bool) {
	var data T
	tableName := struct2name(reflect2.TypeOf(data).String())
	b := hook(psql.Update(tableName)).Suffix("RETURNING *")
	sql, args, _ := b.ToSql()

	row, err := pool.Client.Query(context.Background(), sql, args...)
	ret, err := pgx.CollectOneRow(row, RowToStructByName[T])
	return ret, execErr(err, tableName, "Update")
}

// UpdateTx 为某表更新记录 返回是否成功
//
// 文档地址 https://github.com/Masterminds/squirrel
func UpdateTx[T any](tx pgx.Tx, hook func(sq.UpdateBuilder) sq.UpdateBuilder) bool {
	var data T
	tableName := struct2name(reflect2.TypeOf(data).String())
	b := hook(psql.Update(tableName)).Suffix("RETURNING *")
	sql, args, _ := b.ToSql()

	_, err := tx.Exec(context.Background(), sql, args...)
	return execErr(err, tableName, "Update")
}

// Delete 为某表删除记录 返回是否成功
//
// 文档地址 https://github.com/Masterminds/squirrel
func Delete[T any](hook func(sq.DeleteBuilder) sq.DeleteBuilder) bool {
	var data T
	tableName := struct2name(reflect2.TypeOf(data).String())
	b := hook(psql.Delete(tableName))
	sql, args, _ := b.ToSql()
	_, err := pool.Client.Exec(context.Background(), sql, args...)
	return execErr(err, tableName, "Delete")
}

type Cte[T sq.Sqlizer] struct {
	Alias string
	Expr  T
}

func NewCte[T sq.Sqlizer](alias string, expr T) Cte[T] {
	return Cte[T]{Alias: alias, Expr: expr}
}

func WithCte[T sq.Sqlizer](first Cte[T], rest ...Cte[T]) T {
	var lastExpr T
	first.Expr = builder.Append(first.Expr, "Prefixes",
		sq.Expr(fmt.Sprintf(`WITH "%s" AS (`, first.Alias))).(T)

	lastExpr = builder.Append(first.Expr, "Suffixes",
		sq.Expr(")")).(T)

	for _, cte := range rest {
		cte.Expr = builder.Append(cte.Expr, "Prefixes", lastExpr).(T)

		cte.Expr = builder.Append(cte.Expr, "Prefixes",
			sq.Expr(fmt.Sprintf(`, "%s" AS (`, cte.Alias))).(T)

		lastExpr = builder.Append(cte.Expr, "Suffixes",
			sq.Expr(")")).(T)
	}
	return lastExpr
}

func ToCte[T sq.Sqlizer](alias string, expr T) T {
	return WithCte(Cte[T]{Alias: alias, Expr: expr})
}

func sanitizePageAndSize(page, size uint64) (uint64, uint64) {
	if page < 1 {
		page = 1
	}
	if size < MinPageElements {
		size = MinPageElements
	}
	return page, size
}

var parseFromTableRegex = regexp.MustCompile(`^\s*"?(?P<table>[^"]+)"?\s+"?(?P<alias>[^"]+)"?\s*$`)

func parseFromTableExpr(fromTable string) (string, string) {
	match := parseFromTableRegex.FindStringSubmatch(fromTable)
	if len(match) != 3 {
		return fromTable, fromTable
	}

	return match[1], match[2]
}
