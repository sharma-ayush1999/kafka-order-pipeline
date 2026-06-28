package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)



type DLQMessage struct {
	OriginalTopic 		string 		`json:"original_topic"`
	Partition 			int 		`json:"partition"`
	Offset 				int64 		`json:"offset"`
	Key 				string 		`json:"key"`
	Value 				string 		`json:"value"`
	Error 				string 		`json:"error"`
	FailedAt 			time.Time `json:"failed_at"`
}

type DLQWriter struct {
	writer *kafka.Writer
}

func NewDLQWriter(brokerAddr, dlqTopic string) *DLQWriter {
	return &DLQWriter{
		writer: &kafka.Writer{
			Addr: kafka.TCP(brokerAddr),
			Topic: dlqTopic,
			Balancer: &kafka.Hash{},
			RequiredAcks: kafka.RequireOne,
		},
	}
}

func (d *DLQWriter) Send(ctx context.Context, originalMsg kafka.Message, processingErr error) error {
	dlq := DLQMessage{
		OriginalTopic: originalMsg.Topic,
		Partition: originalMsg.Partition,
		Offset: originalMsg.Offset,
		Key: string(originalMsg.Key),
		Value: string(originalMsg.Value),
		Error: processingErr.Error(),
		FailedAt: time.Now(),
	}
	bytes, err := json.Marshal(dlq)
	if err != nil {
		return fmt.Errorf("marshall dlw message: %w", err)
	} 

	err = d.writer.WriteMessages(ctx, kafka.Message{
		Key: originalMsg.Key,
		Value: bytes,
	})

	if err != nil {
		return fmt.Errorf("write to dlq: %w", err)
	}

	log.Printf("[DLQ] send failed message to DLQ | partition=%d offset=%d error=%s",
			originalMsg.Partition, originalMsg.Offset, processingErr.Error())		
	return nil
}

func (d *DLQWriter) Close() error {
	return d.writer.Close()
}