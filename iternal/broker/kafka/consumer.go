package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"

	"github.com/NoNamePL/kafka-go-broker/iternal/config"
	mongodb "github.com/NoNamePL/kafka-go-broker/iternal/storage/mongo"
	"github.com/NoNamePL/kafka-go-broker/pkg/models"
	"github.com/segmentio/kafka-go"
)

func StartConsumer(cfg *config.Config, logger *slog.Logger, db *mongodb.OperationStorage) error {
	// init Kafka conf
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{cfg.KafkaConfig.Address},
		Topic:     cfg.KafkaConfig.KafkaTopic,
		GroupID:   cfg.KafkaConfig.KafkaGroupID,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
		Partition: 0,
	})
	defer kafkaReader.Close()

	// Create a channel to signal when to stop 
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case <-signals:
			logger.Info("Shutting down consumer...")
			return nil
		default:
			msg, err := kafkaReader.ReadMessage(context.TODO())
			if err != nil {
				logger.Error("Error reading message:", "error", err)
				continue
			}

			var notification models.Notification
			if err := json.Unmarshal(msg.Value, &notification); err != nil {
				logger.Error("Error unmarshaling notification:", "error", err)
				continue
			}

			// Store message in MongoDB
			_, err = db.Collection.InsertOne(context.TODO(), notification)
			if err != nil {
				logger.Error("Error storing message in MongoDB:", "error", err)
				continue
			}

			logger.Info("Processed message:", "notification id", notification.ID)

			// Commit the offset
			if err := kafkaReader.CommitMessages(context.TODO(), msg); err != nil {
				logger.Error("Error committing message:", "error", err)
			}
		}
	}

}
