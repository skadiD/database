package database

import (
	"context"
	"errors"
	"fmt"
	"github.com/Masterminds/squirrel"
	"os"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/fexli/logger"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	dbLog = logger.GetLogger("db", true)
	pool  *Client
)

type Client struct {
	Client      *pgxpool.Pool
	cachedTypes []*pgtype.Type
}

// NewClient 初始化数据库
func NewClient(connString string) *Client {
	var c = &Client{}
	//connString := "postgres://" + c.User + ":" + c.Pass + "@" + c.Host + ":" + c.Port + "/" + c.Name + "?timezone=Asia/Shanghai"
	{
		conn, err := pgx.Connect(context.Background(), connString)
		if err != nil {
			dbLog.Error(logger.WithContent("PgSQL 预载自定义类型时连接失败：", err))
			os.Exit(1)
		}
		defer conn.Close(context.Background())

		var tableNames []string
		err = pgxscan.Select(context.Background(), conn, &tableNames, `
SELECT t.table_name
FROM information_schema.tables t
LEFT JOIN pg_inherits pi ON pi.inhrelid = t.table_name::regclass
WHERE t.table_schema = 'public'
  AND t.table_type = 'BASE TABLE'
  AND pi.inhrelid IS NULL
ORDER BY TABLE_NAME
`)
		if err != nil {
			dbLog.Error(logger.WithContent("PgSQL 预载自定义类型时获取表名失败：", err))
			os.Exit(1)
		}
		dbLog.Notice(logger.WithContent("PgSQL 正在为", len(tableNames), "张表预载record类型"))
		types, err := conn.LoadTypes(context.Background(), tableNames)
		if err != nil {
			dbLog.Error(logger.WithContent("PgSQL 预载自定义类型时获取类型失败：", err))
			os.Exit(1)
		}

		c.cachedTypes = append(c.cachedTypes, types...)
		dbLog.Notice(logger.WithContent("PgSQL 预载自定义类型完成，类型数：", len(c.cachedTypes)))
	}

	dbLog.Notice(logger.WithContent("PgSQL 初始化中"))
	config, _ := pgxpool.ParseConfig(connString)
	config.MinConns = 2
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		conn.TypeMap().RegisterTypes(c.cachedTypes)
		return nil
	}
	var err error
	c.Client, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		dbLog.Error(logger.WithContent("PgSQL 连接失败：", err))
		os.Exit(1)
	}
	dbLog.System(logger.WithContent("PgSQL 连接成功"))
	return c
}

// Types 注册自定义类型
func (c *Client) Types(types []*pgtype.Type) *Client {
	c.cachedTypes = types
	return c
}

// Select 查询多条
func (c *Client) Select(sb squirrel.SelectBuilder, result any) error {
	sql, args, err := sb.ToSql()
	if err != nil {
		execErr(errors.Join(err, errors.New("error building SQL")), "", "database.Select")
		return err
	}

	err = pgxscan.Select(context.Background(), c.Client, &result, sql, args...)
	if err != nil {
		err = errors.Join(err,
			fmt.Errorf("error executing SQL:\n#### SQL:\n%s\n#### Args:\n%v", sql, args))
		execErr(err, "", "database.Select")
	}
	return err
}

// Get 查询单条
func (c *Client) Get(sb squirrel.SelectBuilder, result any) error {
	sql, args, err := sb.ToSql()
	if err != nil {
		execErr(errors.Join(err, errors.New("error building SQL")), "", "database.Get")
	}
	err = pgxscan.Get(context.Background(), c.Client, &result, sql, args...)
	if err != nil {
		err = errors.Join(err,
			fmt.Errorf("error executing SQL:\n#### SQL:\n%s\n#### Args:\n%v", sql, args))
		execErr(err, "", "database.Get")
	}
	return err
}

// Update 修改数据
func (c *Client) Update(sb squirrel.UpdateBuilder) (int64, error) {
	sql, args, err := sb.ToSql()
	if err != nil {
		execErr(errors.Join(err, errors.New("error building SQL")), "", "database.Update")
	}

	cmd, err := c.Client.Exec(context.Background(), sql, args...)
	if err != nil {
		err = errors.Join(err,
			fmt.Errorf("error executing SQL:\n#### SQL:\n%s\n#### Args:\n%v", sql, args))
		execErr(err, "", "database.Update")
	}

	return cmd.RowsAffected(), err
}

// Delete 删除数据
func (c *Client) Delete(sb squirrel.DeleteBuilder) (int64, error) {
	sql, args, err := sb.ToSql()
	if err != nil {
		execErr(errors.Join(err, errors.New("error building SQL")), "", "database.Delete")
	}

	cmd, err := c.Client.Exec(context.Background(), sql, args...)
	if err != nil {
		err = errors.Join(err,
			fmt.Errorf("error executing SQL:\n#### SQL:\n%s\n#### Args:\n%v", sql, args))
		execErr(err, "", "database.Delete")
	}

	return cmd.RowsAffected(), err
}

// Insert 插入数据
func (c *Client) Insert(sb squirrel.InsertBuilder) (int64, error) {
	sql, args, err := sb.ToSql()
	if err != nil {
		execErr(errors.Join(err, errors.New("error building SQL")), "", "database.Insert")
	}

	cmd, err := c.Client.Exec(context.Background(), sql, args...)
	if err != nil {
		err = errors.Join(err,
			fmt.Errorf("error executing SQL:\n#### SQL:\n%s\n#### Args:\n%v", sql, args))
		execErr(err, "", "database.Insert")
	}

	return cmd.RowsAffected(), err
}
