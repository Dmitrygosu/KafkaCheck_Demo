package repository

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/IBM/sarama"

	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/config"
	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/domain"
	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/pkg/api"
)

// ProducerRepository publishes domain messages through an async Sarama producer.
type ProducerRepository struct {
	producer sarama.AsyncProducer
	topic    string
	logger   *slog.Logger
	wg       sync.WaitGroup

	// mu guards closed. SendMessage holds it for reading while it writes to
	// the input channel, so Close cannot close that channel under a send.
	mu     sync.RWMutex
	closed bool

	delivered atomic.Int64
	failed    atomic.Int64
}

func NewProducerRepository(brokers []string, version sarama.KafkaVersion, cfg *config.ProducerConfig,
	logger *slog.Logger) (*ProducerRepository, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = version
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true

	switch cfg.RequiredAcks {
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

	saramaCfg.Producer.Timeout = config.GetDuration(cfg.Timeout, "s")
	saramaCfg.Producer.MaxMessageBytes = cfg.MaxMessageBytes
	saramaCfg.Producer.Flush.Messages = cfg.FlushMessages
	saramaCfg.Producer.Flush.Frequency = config.GetDuration(cfg.FlushFrequency, "ms")
	saramaCfg.Producer.Retry.Max = cfg.RetryMax
	saramaCfg.Producer.Retry.Backoff = config.GetDuration(cfg.RetryBackoff, "ms")

	producer, err := sarama.NewAsyncProducer(brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}
	return newProducerRepository(producer, cfg.Topic, logger), nil
}

func newProducerRepository(producer sarama.AsyncProducer, topic string, logger *slog.Logger) *ProducerRepository {
	p := &ProducerRepository{producer: producer, topic: topic, logger: logger}
	p.startResultHandlers()
	return p
}

// startResultHandlers drains the success and error channels; Sarama blocks
// if they are not read.
func (p *ProducerRepository) startResultHandlers() {
	p.wg.Add(2)

	go func() {
		defer p.wg.Done()
		for msg := range p.producer.Successes() {
			p.delivered.Add(1)
			p.logger.Debug("message delivered", "partition", msg.Partition, "offset", msg.Offset)
		}
	}()

	go func() {
		defer p.wg.Done()
		for err := range p.producer.Errors() {
			p.failed.Add(1)
			p.logger.Error("message delivery failed", "error", err.Err)
		}
	}()
}

// SendMessage enqueues msg; delivery is reported asynchronously.
func (p *ProducerRepository) SendMessage(msg *domain.Message) error {
	data, err := api.SerializeMessage(msg)
	if err != nil {
		return fmt.Errorf("serialize: %w", err)
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return domain.ErrKafkaProducerClosed
	}
	p.producer.Input() <- &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(msg.ID),
		Value: sarama.ByteEncoder(data),
	}
	return nil
}

// GetMetrics returns the number of delivered and failed messages.
func (p *ProducerRepository) GetMetrics() (delivered, failed int64) {
	return p.delivered.Load(), p.failed.Load()
}

// Close flushes buffered messages and waits for their delivery reports.
func (p *ProducerRepository) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	err := p.producer.Close()
	p.wg.Wait()
	return err
}
