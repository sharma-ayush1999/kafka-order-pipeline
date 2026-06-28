package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	groupID = "notification-group"
)

func main(){
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic: orderTopic,
		GroupID: groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	ctx, Cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func(){
		<-sigChan
		log.Printf("shutdown signal received")
		Cancel()
	}()

	log.Printf("notification-service started | group=%s topic=%s", groupID, orderTopic)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("ERROR fetching: %v", err)
			continue
		}

		var order models.Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Printf("ERROR deserializing offset=%d: %v", msg.Offset, err)
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("ERROR committing bad message: %v", err)
			}
			continue
		}
		if err := sendNotification(order, msg); err != nil {
			log.Printf("ERROR sending notification order=%s: %v", order.ID, err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("ERROR committing offset=%d: %v", msg.Offset, err)
		}
	}
	log.Printf("notification-service stopped")
}

func sendNotification(order models.Order, msg kafka.Message) error {
	log.Printf("[NOTIFICATION] email sent -> user=%s order=%s item=%s amount=%.2f | partition=%d offset=%d",
			order.UserID, order.ID, order.Item, order.Amount, msg.Partition, msg.Offset)	
	fmt.Printf("  📧 Dear %s, your order for %s is confirmed!\n", order.UserID, order.Item)
	return nil
}