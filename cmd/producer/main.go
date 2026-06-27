package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
	kafkaadmin "github.com/sharma-ayush1999/kafka-order-pipeline/internal/kafka"
	"github.com/sharma-ayush1999/kafka-order-pipeline/internal/models"
)

const (
	brokerAddr = "localhost:9092"
	orderTopic = "orders"
)

var writer *kafka.Writer

func main(){
	if err := kafkaadmin.CreateTopic(brokerAddr, orderTopic, 3, 1); err != nil {
		log.Fatalf("failed to create topic: %v", err)
	}

	writer = &kafka.Writer{
		Addr: kafka.TCP(brokerAddr),
		Topic: orderTopic,
		Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	http.HandleFunc("/order", handleOrder)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request){
		w.WriteHeader(http.StatusOK)
	})

	log.Println("producer listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func handleOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost  {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var order models.Order
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	order.ID = fmt.Sprintf("order-%d", time.Now().UnixNano())
	order.Status = models.StatusPending
	order.CreatedAt = time.Now()

	valueBytes, err := json.Marshal(order)
	if err != nil {
		http.Error(w, "serialization failed", http.StatusInternalServerError)
	}

	msg := kafka.Message{
		Key: []byte(order.ID),
		Value: valueBytes,
		Headers: []kafka.Header{
			{Key:"source", Value: []byte("order-api")},
		},
	}

	if err := writer.WriteMessages(context.Background(), msg); err != nil {
		log.Printf("ERROR publishing order %s: %v", order.ID, err)
		http.Error(w, "failed to publish", http.StatusInternalServerError)
		return
	}

	log.Printf("PUBLISHED order_id=%s item=%s qty=%d", order.ID, order.Item, order.Quantity)

	w.Header().Set("Content-Type", "application_json")
	json.NewEncoder(w).Encode(map[string]string{
		"order_id": order.ID,
		"status": string(order.Status),
	})
}