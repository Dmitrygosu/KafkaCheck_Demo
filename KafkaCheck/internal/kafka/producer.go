package kafka

import "github.com/IBM/sarama"

// Producer отправляет соо в кафку
type Producer struct {
	asyncProducer sarama.AsyncProducer
}

// NewProducer создает новый продюсер
func NewProducer(brokers []string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	asyncProducer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &Producer{asyncProducer: asyncProducer}, nil
}

// SendMessage отправляет соо в указанный топик
func (p *Producer) SendMessage(topic string, key []byte, message []byte) {
	p.asyncProducer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(message),
	}
}

// Close завершает работу продюсера
func (p *Producer) Close() error {
	return p.asyncProducer.Close()
}
