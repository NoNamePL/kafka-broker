package models

import "time"

type Notification struct {
	ID        string    `json:"id" bson:"id"`
	Data      string    `json:"data" bson:"data"`
	Timestamp time.Time `json:"timestamp" bson:"timestamp"`
}
