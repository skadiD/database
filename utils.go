package database

import (
	"database/sql"
	"errors"
	"github.com/fexli/logger"
	"reflect"
	"strings"
)

// execErr 数据表错误统一处理 无错误返回 true
func execErr(err error, table, action string, model ...any) bool {
	if err != nil {
		if strings.HasPrefix(err.Error(), "ERROR: failed to connect to") {
			return false
		}
		if !errors.Is(err, sql.ErrNoRows) && !strings.HasPrefix(err.Error(), "ERROR: duplicate key") {
			if model != nil {
				dbLog.Debug(logger.WithContent(model))
			}
			dbLog.Warning(logger.WithContent("【"+table+"】<"+action+">错误"), logger.WithContent(GetFormatTrace(err, 5, false, false)), logger.WithBacktraceLevelDelta(2))
		}
		return false
	}
	return true
}

func dbLogPrint(content ...logger.LogCtx) {
	dbLog.Warning(logger.WithContent(content), logger.WithContent(GetFormatTrace(nil, 5, false, false)), logger.WithBacktraceLevelDelta(2))
}

func IsZeroValue(value any) bool {
	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		return v == 0
	case uint, uint8, uint16, uint32, uint64:
		return v == 0
	case float32, float64:
		return v == 0.0
	case bool:
		return !v
	case string:
		return v == ""
	default:
		var zeroValue any
		return reflect.DeepEqual(value, zeroValue)
	}
}
