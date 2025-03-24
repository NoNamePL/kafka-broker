package main

import (
	"context"
	"log"

	"github.com/NoNamePL/kafka-go-broker/iternal/broker/kafka"
	"github.com/NoNamePL/kafka-go-broker/iternal/config"
	"github.com/NoNamePL/kafka-go-broker/iternal/logger"
	mongodb "github.com/NoNamePL/kafka-go-broker/iternal/storage/mongo"
)

func main() {

	// init logger
	logger, err := logger.InitLogger("Kafka-Broker")
	if err != nil {
		log.Fatal(err)
	}

	// init config
	cfg, err := config.NewConfig()
	if err != nil {
		logger.Error("Error loading .env file", "error",err.Error())
		return
	}

	// init mongoDB
	operationsCollection, err := mongodb.NewOperationStorage(cfg)
	if err != nil {
		logger.Error("Error connecting to MongoDB","error", err.Error())
		return
	}
	defer operationsCollection.Collection.Database().Client().Disconnect(context.Background())

	logger.Info("Successfully connected to MongoDB")

	// init broker

	err = kafka.StartConsumer(cfg,logger,operationsCollection)
	if err != nil {
		logger.Error("Error in broker", "error", err.Error())
		return
	}

}
