package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
	"github.com/sharma-ayush1999/kafka-order-pipeline/internal/models"
)

const (
	brokerAddr = "localhost:9092"
	orderTopic = "orders"
	groupID = "inventory-group"
)

func main(){
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic: orderTopic,

        // GroupID makes this a consumer group member
		// Kafka tracks committed offsets per groupID independently
		GroupID: groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
		
		// FirstOffset: if this group has never connected before, start from oldest message
        // Once offsets are committed, Kafka ignores this and resumes from last commit
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	ctx, Cancel := context.WithCancel(context.Background())
	
	// handle Ctrl+C and SIGTERM (Kubernetes sends SIGTERM on pod shutdown)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func(){
		<-sigChan
		log.Printf("shutdown signal received")
		Cancel()
	}()

	log.Printf("inventory-service started | group=%s topic=%s", groupID, orderTopic)

	for {
		// blocks until a message arrives or context is cancelled
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // clean shutdown
			}
			log.Printf("ERROR reading: %v", err)
			continue;
		}

		var order models.Order

		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Printf("ERROR deserializing offset=%d: %v", msg.Offset, err)
			continue
		}

		log.Printf("[INVENTORY] order_id=%s item=%s qty=%d | partition=%d offset=%d", order.ID, order.Item, order.Quantity, msg.Partition, msg.Offset)
	}
	log.Printf("inventory service stopped")
}