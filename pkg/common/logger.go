package common

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
)

var GloablDbDir string = ""

type Logger struct {
	logFile  *os.File
	logEmoji rune
	logLatch sync.RWMutex
}

func NewLogger(emoji rune) *Logger {
	os.MkdirAll(GloablDbDir, 0755)
	logPath := path.Join(GloablDbDir, "elena.log")
	// create if not exists. don't flush on open
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

	if err != nil {
		panic(err)
	}

	return &Logger{
		logFile:  logFile,
		logEmoji: emoji,
		logLatch: sync.RWMutex{},
	}
}

func (l *Logger) Log(level string, format string, a ...any) {
	l.logLatch.Lock()
	defer l.logLatch.Unlock()

	newline := ""
	if strings.HasPrefix(format, "\n") {
		newline = "\n"
		format = strings.TrimPrefix(format, "\n")
	}

	now := time.Now()
	_, err := l.logFile.WriteString(
		fmt.Sprintf(
			"%v%v [%v] %v %v\n",
			newline, now.Format("15:04:05"), level, string(l.logEmoji), fmt.Sprintf(format, a...),
		),
	)
	l.logFile.Sync()
	if err != nil {
		panic(err)
	}
}

func (l *Logger) Boot(format string, a ...any) {
	l.Log(fmt.Sprintf(color.New(color.FgMagenta).Sprint("BOOT")), format, a...)
}

func (l *Logger) Info(format string, a ...any) {
	l.Log(fmt.Sprintf(color.New(color.FgGreen).Sprint("INFO")), format, a...)
}

func (l *Logger) Error(format string, a ...any) {
	l.Log(fmt.Sprintf(color.New(color.FgRed).Sprint(" ERR")), format, a...)
}

func (l *Logger) Warn(format string, a ...any) {
	l.Log(fmt.Sprintf(color.New(color.FgYellow).Sprint("WARN")), format, a...)
}

func (l *Logger) Debug(format string, a ...any) {
	l.Log(fmt.Sprintf(color.New(color.FgBlue).Sprint("DEBG")), format, a...)
}
