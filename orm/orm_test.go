package orm

import (
	"testing"

	"github.com/skadiD/database"
)

type User struct {
	ID   int64  `orm:"id,pk,auto"`
	Name string `orm:"name"`
	Age  int    `orm:"age"`
}

func TestOrm_Update(t *testing.T) {
	// 预制一个 user 对象
	user := &User{
		ID:   1,
		Name: "test",
		Age:  18,
	}
	_ = database.RegisterModel[User]("user")
	t.Log(Model[User](nil).Load(user).Update())
}

// cpu: AMD Ryzen 9 9950X 16-Core Processor
// BenchmarkOrm_Update
// BenchmarkOrm_Update-32    	  384664	      3237 ns/op
func BenchmarkOrm_Update(b *testing.B) {
	// 预制一个 user 对象
	user := &User{
		ID:   1,
		Name: "test",
		Age:  18,
	}
	_ = database.RegisterModel[User]("user")
	b.ResetTimer()

	u := Model[User](nil).Load(user)
	for i := 0; i < b.N; i++ {
		u.Update()
	}
}

// cpu: AMD Ryzen 9 9950X 16-Core Processor
// BenchmarkRaw_Update
// BenchmarkRaw_Update-32    	  418981	      2623 ns/op
func BenchmarkRaw_Update(b *testing.B) {
	user := &User{
		ID:   1,
		Name: "test",
		Age:  18,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		psql.Update("user").Set("name", user.Name).Set("age", user.Age).Where("id = ?", user.ID).ToSql()
	}
}
