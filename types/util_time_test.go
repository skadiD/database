package types

import (
	"testing"
	"time"
)

func TestGetMinutesOfWeek(t *testing.T) {
	inTime, err := time.Parse("2006-01-02 15:04:05", "2024-10-28 00:00:00")
	if err != nil {
		t.Fatal(err)
	}

	minutes := GetMinutesOfWeek(inTime)
	if minutes != 0 {
		t.Fatal("expected 0, got", minutes)
	}

	inTime, err = time.Parse("2006-01-02 15:04:05", "2024-10-30 08:30:30")
	if err != nil {
		t.Fatal(err)
	}

	minutes = GetMinutesOfWeek(inTime)
	if minutes != 3390 {
		t.Fatal("expected 3390, got", minutes)
	}

	inTime, err = time.Parse("2006-01-02 15:04:05", "2024-11-03 23:59:59")
	if err != nil {
		t.Fatal(err)
	}

	minutes = GetMinutesOfWeek(inTime)
	if minutes != 10079 {
		t.Fatal("expected 10079, got", minutes)
	}
}
