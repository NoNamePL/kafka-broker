// Продюсер должен быть в сервисе кошельке
// Тут изображен пример

package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/NoNamePL/kafka-go-broker/iternal/config"
	mongodb "github.com/NoNamePL/kafka-go-broker/iternal/storage/mongo"
	"github.com/NoNamePL/kafka-go-broker/pkg/models"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

func StartProducer(cfg *config.Config, logger *slog.Logger, db *mongodb.OperationStorage) {

	// Initialize Kafka writer
	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaConfig.Address),
		Topic:    cfg.KafkaConfig.KafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	// Create a channel to signal when to stop producing
	stopCh := make(chan struct{})

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	messageCount := 0

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			messageCount++

			// Create a new message
			message := models.Notification{
				ID:        uuid.New().String(),
				Data:      fmt.Sprintf("Message #%d", messageCount),
				Timestamp: time.Now(),
			}

			// Marshal the message to JSON
			data, err := json.Marshal(message)
			if err != nil {
				logger.Error("Error marshaling message:", "error", err)
				continue
			}

			// Write the message to Kafka
			err = kafkaWriter.WriteMessages(context.TODO(), kafka.Message{
				Key:   []byte(message.ID),
				Value: data,
			})

			if err != nil {
				logger.Error("Error writing message to Kafka:", "error", err)
				continue
			}

			logger.Info("Produced message:", "message id", message.ID)
		}
	}
}
