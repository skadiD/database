package types

import (
	"strconv"
	"strings"
)

type PublicId int64

func (p *PublicId) Scan(data interface{}) error {
	if data == nil {
		return nil
	}
	*p = PublicId(data.(int64))
	return nil
}

func (p PublicId) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strconv.FormatInt(int64(p), 10) + `"`), nil
}

func (p *PublicId) UnmarshalJSON(b []byte) error {
	temp := strings.Trim(string(b), `"`)
	pp, _ := strconv.ParseInt(temp, 10, 64)
	*p = PublicId(pp)
	return nil
}
