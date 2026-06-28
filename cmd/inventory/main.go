package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	kafkapkg "github.com/sharma-ayush1999/kafka-order-pipeline/internal/kafka"

	"github.com/segmentio/kafka-go"
	"github.com/sharma-ayush1999/kafka-order-pipeline/internal/models"
)

const (
	brokerAddr = "localhost:9092"
	orderTopic = "orders"
	dlqTopic = "orders.dlq"
	groupID = "inventory-group"
)

func main(){
	// create DLQ topic on startup
	if err := kafkapkg.CreateTopic(brokerAddr, dlqTopic, 3, 1); err != nil {
		log.Fatalf("failed to created dlq topic: %v", err)
	}

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

	dlq := kafkapkg.NewDLQWriter(brokerAddr, dlqTopic)
	defer dlq.Close()

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
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("Error fetching: %v", err)
			continue
		}
		var order models.Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {

			dlq.Send(ctx, msg, err)
			reader.CommitMessages(ctx, msg)
			continue
		}

		if err := processOrder(order, msg); err != nil {
			dlq.Send(ctx, msg, err)
			reader.CommitMessages(ctx, msg)
			continue
		}
	}
	log.Printf("inventory service stopped")
}

func processOrder(order models.Order, msg kafka.Message) error {
	//failure simulation
	if order.Quantity > 8 {
		return errors.New("insufficient stock")
	}
	log.Printf("[INVENTORY] order_id=%s item=%s qty=%d | partition=%d offset=%d",
			order.ID, order.Item, order.Quantity, msg.Partition, msg.Offset)
	return nil
} 