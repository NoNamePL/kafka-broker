package config

import (
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	// DB
	MongoConn string

	// kafka
	KafkaConfig kafkaConfig
}

type kafkaConfig struct{
	Address string
	KafkaTopic string
}

func NewConfig() (*Config, error) {

	err := godotenv.Load()

	if err != err {
		return nil, err
	}

	storage := Config{
		MongoConn: os.Getenv("DB_PATH"),
		KafkaConfig: kafkaConfig{
			Address: os.Getenv("KafkaServerAddress"),
			KafkaTopic: os.Getenv("KafkaTopic"),
		},
	}
	return &storage, nil
}
