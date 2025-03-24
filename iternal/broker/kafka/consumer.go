package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"

	"github.com/IBM/sarama"
	"github.com/NoNamePL/kafka-go-broker/iternal/config"
	mongodb "github.com/NoNamePL/kafka-go-broker/iternal/storage/mongo"
)

func StartConsumer(cfg *config.Config, logger *slog.Logger, db *mongodb.OperationStorage) error {
	// Адреса брокеров Kafka
	brokers := []string{cfg.KafkaConfig.Address}

	// Настройка конфигурации consumer'а
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Создание нового consumer'а
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		logger.Error("Ошибка при создании consumer", "error", err)
		return err
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("Ошибка при закрытии consumer", "error", err)
			return
		}
	}()

	// Топик, который будем слушать
	topic := cfg.KafkaConfig.KafkaTopic

	// Получение партиций для топика
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		logger.Error("Ошибка при получении партиций:", "error", err)
		return err
	}

	// Канал для обработки сигналов завершения
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// WaitGroup для ожидания завершения всех горутин
	var wg sync.WaitGroup

	// Чтение сообщений из каждой партиции
	for _, partition := range partitions {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()

			// Создание partition consumer'а
			pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
			if err != nil {
				logger.Error("Ошибка при создании partition consumer", "error", err)
				return
			}
			defer pc.AsyncClose()

			// Чтение сообщений
			for {
				select {
				case msg := <-pc.Messages():
					db.Collection.InsertOne(context.TODO(),msg.Partition)
					fmt.Printf("Получено сообщение: Partition: %d, Offset: %d, Key: %s, Value: %s\n",
						msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
				case err := <-pc.Errors():
					logger.Error("Ошибка при чтении сообщения:", "error", err)
				case <-signals:
					return
				}
			}
		}(partition)
	}

	// Ожидание завершения всех горутин
	wg.Wait()
	logger.Info("Consumer завершил работу")
	return nil
}
