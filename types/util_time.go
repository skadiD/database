package types

import "time"

// GetMinutesOfWeek 获取一周内的分钟时刻
func GetMinutesOfWeek(time time.Time) int {
	weekday := int(time.Weekday())
	// 偏移周日开始到周一开始
	if weekday == 0 {
		weekday = 6
	} else {
		weekday--
	}

	minutes := weekday * 1440
	minutes += time.Hour() * 60
	minutes += time.Minute()

	return minutes
}
