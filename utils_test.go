package database

import "testing"

func TestIsZero(t *testing.T) {
	var zeroStr any = ""
	var notZeroStr any = "not zero"
	t.Log(IsZeroValue(zeroStr))
	t.Log(IsZeroValue(notZeroStr))
}
