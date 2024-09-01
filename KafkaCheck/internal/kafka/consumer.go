package kafka

import (
	"github.com/IBM/sarama"
	"log"
)

// Consumer  читает из кафки
type Consumer struct {
	consumer sarama.Consumer
}

// NewConsumer создает консьюмер
func NewConsumer(brokers []string) (*Consumer, error) {
	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		return nil, err
	}

	return &Consumer{consumer: consumer}, nil
}

// ConsumeMessages начинает чтение соо из топика
func (c *Consumer) ConsumeMessages(topic string) {
	partitionConsumer, err := c.consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal("Ошибка чтения из партиции:", err)
	}

	defer partitionConsumer.Close()

	for message := range partitionConsumer.Messages() {
		log.Printf("Сообщение: %s\n", string(message.Value))
	}
}

// Close рубит консьюмер
func (c *Consumer) Close() error {
	return c.consumer.Close()
}
