package repository

import (
	"KafkaCheck/internal/config"
	"KafkaCheck/internal/domain"
	"KafkaCheck/pkg/api"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
)

type ProducerRepository struct {
	producer     sarama.AsyncProducer
	topic        string
	wg           sync.WaitGroup
	closed       bool
	mu           sync.RWMutex
	successCount int64
	errorCount   int64
}

func NewProducerRepository(brokers []string, cfg *config.ProducerConfig) (*ProducerRepository, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true

	switch cfg.RequiredAcks {
	case "WaitForAll":
		saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	case "WaitForLocal":
		saramaCfg.Producer.RequiredAcks = sarama.WaitForLocal
	case "NoResponse":
		saramaCfg.Producer.RequiredAcks = sarama.NoResponse
	default:
		saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	}

	switch cfg.Compression {
	case "gzip":
		saramaCfg.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		saramaCfg.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		saramaCfg.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		saramaCfg.Producer.Compression = sarama.CompressionZSTD
	default:
		saramaCfg.Producer.Compression = sarama.CompressionNone
	}

	saramaCfg.Producer.MaxMessageBytes = cfg.MaxMessageBytes
	saramaCfg.Producer.Flush.Messages = cfg.FlushMessages
	saramaCfg.Producer.Flush.Frequency = config.GetDuration(cfg.FlushFrequency, "ms")
	saramaCfg.Producer.Retry.Max = cfg.RetryMax
	saramaCfg.Producer.Retry.Backoff = config.GetDuration(cfg.RetryBackoff, "ms")

	producer, err := sarama.NewAsyncProducer(brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}

	repo := &ProducerRepository{producer: producer, topic: cfg.Topic}
	repo.startResultHandlers()
	return repo, nil
}

func (p *ProducerRepository) startResultHandlers() {
	p.wg.Add(2)
	
	go func() {
		defer p.wg.Done()
		for msg := range p.producer.Successes() {
			p.successCount++
			log.Printf("[producer] delivered: partition=%d offset=%d", msg.Partition, msg.Offset)
		}
	}()

	go func() {
		defer p.wg.Done()
		for err := range p.producer.Errors() {
			p.errorCount++
			log.Printf("[producer] error: %v", err.Err)
		}
	}()
}

func (p *ProducerRepository) SendMessage(msg *domain.Message) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return domain.ErrKafkaProducerClosed
	}
	p.mu.RUnlock()

	data, err := api.SerializeMessage(msg)
	if err != nil {
		return fmt.Errorf("serialize: %w", err)
	}

	p.producer.Input() <- &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(msg.ID),
		Value: sarama.ByteEncoder(data),
	}
	return nil
}

func (p *ProducerRepository) GetMetrics() (int64, int64) {
	return p.successCount, p.errorCount
}

func (p *ProducerRepository) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	if err := p.producer.Close(); err != nil {
		return err
	}
	p.wg.Wait()
	return nil
}
