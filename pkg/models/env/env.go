package env

import (
	"os"
	"strconv"
)

func GetEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func GetEnvInt(key string, defaultValue int) int {
	valueStr, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}

	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	} else {
		panic(err)
	}
}
