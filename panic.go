package database

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"time"
)

var (
	s    = []byte("/src/runtime/panic.go")
	e    = []byte("\ngoroutine ")
	line = []byte("\n")
)

// GetRawPanicTrace 返回指定KB长度的panic堆栈信息和堆栈信息长度
func GetRawPanicTrace(kb int) (stack []byte, length int) {
	stack = make([]byte, kb<<10) //4KB
	length = runtime.Stack(stack, true)
	return
}

// GetPanicTrace 获取最大长度为kb的panic堆栈信息，解析为string并搜索堆栈内goroutine数量
func GetPanicTrace(kb int) (stackStr string, gorouteLen int) {
	stack, length := GetRawPanicTrace(kb)
	gorouteLen = bytes.Count(stack, e)
	start := bytes.Index(stack, s)
	if start == -1 {
		start = 0
	}
	stack = stack[start:length]
	start = bytes.Index(stack, line) + 1
	stack = stack[start:]
	end := bytes.LastIndex(stack, line)
	if end != -1 {
		stack = stack[:end]
	}
	end = bytes.Index(stack, e)
	if end != -1 {
		stack = stack[:end]
	}
	stack = bytes.TrimRight(stack, "\n")
	stackStr = string(stack)
	return
}

func parseTracePath(info string, hidePath bool) string {
	if !hidePath {
		return "  File " + info[1:]
	}
	strs := strings.Split(info[8:], "/")
	return "  File ~/" + strs[len(strs)-1]
}

// GetFixedPanicTrace 按照易读取的方式返回panic的堆栈信息和goroutine数量
func GetFixedPanicTrace(kb int, hidePath bool) (string, int) {
	stack, gLen := GetPanicTrace(kb)
	if stack == "" {
		return "", gLen
	}
	strSli := strings.Split(stack, "\n")
	resultSli := make([]string, 0, len(strSli))
	tmpMsg := ""
	for _, info := range strSli {
		if info[0] == '\t' {
			resultSli = append(resultSli, parseTracePath(info, hidePath))
			resultSli = append(resultSli, tmpMsg)
		} else {
			tmpMsg = "    " + info
		}
	}
	return strings.Join(resultSli, "\n"), gLen
}

// GetFormatTrace 对panic堆栈信息进行格式化返回可进行Log的panic信息，r为recovery()返回的信息，kb为获取的堆栈信息的长度，hidePath为
// 是否隐藏路径信息，hideGor为是否隐藏goroutine信息
func GetFormatTrace(r interface{}, kb int, hidePath bool, hideGor bool) string {
	trace, _ := GetFixedPanicTrace(kb, hidePath)
	timeFmt := time.Now().Format("2006-01-02 15:04:05")
	errHead := fmt.Sprintf("\n程序于%s捕获到异常\n      异常类型：%T\n      异常描述：", timeFmt, r)
	errHead += fmt.Sprintf("%v", r)
	if !hideGor {
		errHead += fmt.Sprintf("\n运行中的协程量：%d", runtime.NumGoroutine())
	}
	errInfo := "\n异常触发栈追踪：\n" + trace
	return errHead + errInfo
}
