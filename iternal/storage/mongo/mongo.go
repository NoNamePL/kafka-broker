package mongodb

import (
	"context"

	"github.com/NoNamePL/kafka-go-broker/iternal/config"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type OperationStorage struct {
	Collection *mongo.Collection
}

func NewOperationStorage(cfg *config.Config) (*OperationStorage, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.MongoConn))
	if err != nil {
		panic(err)
	}

	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		panic(err)
	}

	operationsCollection := client.Database("core").Collection("operations")
	return &OperationStorage{
		Collection: operationsCollection,
	},nil
}