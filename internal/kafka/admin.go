package kafka

import (
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)




func CreateTopic(brokerAddr, topic string, partitions, replicationFactor int) error {
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller %w", err)
	}

	ctrlAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	ctrlConn, err := kafka.Dial("tcp", ctrlAddr)
	if err != nil {
		return fmt.Errorf("dial controller %w", err)
	}
	defer ctrlConn.Close()

	err = ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic: topic,
		NumPartitions: partitions,
		ReplicationFactor: replicationFactor,
	})

	if err != nil {
		if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr == kafka.TopicAlreadyExists {
			fmt.Printf("topic %q already exists, skipping\n", topic)
			return nil
		}
		return fmt.Errorf("create topic %w", err)
	}

	fmt.Printf("created topic %q | partition=%d | replication=%d", topic, partitions, replicationFactor);
	return nil
}