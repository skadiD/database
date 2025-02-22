# 使用指南

本库提供简洁的数据库操作接口，支持结构体与数据库表的映射、链式查询构建、零值过滤等功能。以下是快速入门指南。

---

## 前置条件

### 1. 模型注册
**必须在使用前注册模型**，以便 ORM 自动处理表结构映射：

```go
import "github.com/skadiD/database"

// 定义模型结构体
type User struct {
    ID   int64  `orm:"id,pk,auto"`    // 使用 orm 标签指定列名 pk 主键 auto 自增
    Name string `db:"name"`
}

// 在 init 函数或包初始化阶段注册
func init() {
    // 注册模型：[类型参数] + 表名
    _ = database.RegisterModel[User]("users")
}
```
注意：
- 必须为每个模型调用 RegisterModel
- 表名需与数据库实际表名一致

## 快速开始
### 1. 初始化 ORM

```go
import "github.com/skadiD/database"

func main() {
    // 创建模型实例
    user := User{Name: "Alice"}
    
    // 初始化 ORM 实例
    o := orm.Model[User](c).Load(&user)
}
```

### 2. 查询操作
```go
// 查询所有字段
selector := o.Select()

// 自定义查询字段
selector := o.Select("id", "name")

// 添加条件
selector.Where(squirrel.Gt{"age": 18}).
    Where(squirrel.Like{"name": "%A%"})

// 排序和分页
selector.OrderBy("created_at DESC").
    Limit(10).
    Offset(0)

// 获取多条记录
results, err := selector.Get()

// 获取单条记录
user, err := selector.One()
```

### 3. 更新操作
```go
// 自动过滤零值
updater := o.Update().
    Where(squirrel.Eq{"id": 123})

// 强制更新（包括零值）
affected, err := o.Save()

// 按需更新
affected, err := o.Update()
```

### 4. 插入操作
```go
affected, err := o.Insert()
```

### 5. 删除操作
```go
affected, err := o.Delete()
```

---

## TODO
- [ ] 支持事务
- [ ] 支持多表查询
- [ ] 支持代码生成
---

## 贡献与反馈
如有问题或建议，请提交 Issue 或 Pull Request。