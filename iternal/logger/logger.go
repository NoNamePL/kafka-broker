package logger

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/gin-gonic/gin"
)

func InitLogger(service string) (*slog.Logger, error) {

	file, err := os.OpenFile("info.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)

	if err != nil {
		return nil, err
	}

	defer file.Close()

	logger := slog.New(slog.NewJSONHandler(io.MultiWriter(file, os.Stdout), nil))

	logger.Debug(fmt.Sprintf("Service %s======================== Debug message", service))
	logger.Info(fmt.Sprintf("Service %s======================== Info message", service))
	logger.Warn(fmt.Sprintf("Service %s======================== Warning message", service))
	logger.Error(fmt.Sprintf("Service %s======================== Error message", service))

	slog.With("gin_mode", gin.EnvGinMode)
	return logger, nil
}
