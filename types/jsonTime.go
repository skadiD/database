package types

import (
	"database/sql/driver"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"time"
)

type JsonTime time.Time
type ANY struct{}

func (j JsonTime) MarshalJSON() ([]byte, error) {
	return []byte(time.Time(j).Format(`"2006-01-02 15:04:05"`)), nil
}

func (j *JsonTime) UnmarshalJSON(b []byte) error {
	t, err := time.Parse(`"2006-01-02 15:04:05"`, string(b))
	if err != nil {
		return err
	}

	*j = JsonTime(t)
	return nil
}

func (j *JsonTime) Scan(data interface{}) error {
	if data == nil {
		return nil
	}
	*j = JsonTime(data.(time.Time))
	return nil
}

func (j JsonTime) Value() (driver.Value, error) {
	return j.ToTime(), nil
}

func (j JsonTime) ToTime() time.Time {
	return time.Time(j)
}

type ZeroNullJsonTime zeronull.Timestamptz

func (j ZeroNullJsonTime) MarshalJSON() ([]byte, error) {
	t := time.Time(j)
	if t.IsZero() {
		return []byte("null"), nil
	}

	return []byte(t.Format(`"2006-01-02 15:04:05"`)), nil
}
func (j *ZeroNullJsonTime) UnmarshalJSON(b []byte) error {
	if b == nil {
		return nil
	}

	t, err := time.Parse(`"2006-01-02 15:04:05"`, string(b))
	if err != nil {
		return err
	}

	*j = ZeroNullJsonTime(t)
	return nil
}
func (j *ZeroNullJsonTime) Scan(data any) error {
	if data == nil {
		return nil
	}
	return (*zeronull.Timestamptz)(j).Scan(data)
}

func (j ZeroNullJsonTime) Value() (driver.Value, error) {
	return zeronull.Timestamptz(j).Value()
}

func (j ZeroNullJsonTime) ToTime() time.Time {
	return time.Time(j)
}
