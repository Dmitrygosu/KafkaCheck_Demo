package repository

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/IBM/sarama"

	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/config"
	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/domain"
	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/pkg/api"
)

type ConsumerRepository struct {
	group   sarama.ConsumerGroup
	topics  []string
	handler *groupHandler
	logger  *slog.Logger
	closed  bool
	mu      sync.RWMutex
}

// groupHandler marks every message after handling it, including messages
// that fail to decode or whose callback fails: the demo favors progress
// over redelivery (at-most-once for failed messages).
type groupHandler struct {
	callback  func(*domain.Message) error
	logger    *slog.Logger
	processed int64
	errors    int64
	mu        sync.Mutex
}

func (h *groupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *groupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			h.processMessage(session, msg)
		}
	}
}

func (h *groupHandler) processMessage(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
	domainMsg, err := api.DeserializeMessage(msg.Value)
	if err != nil {
		h.mu.Lock()
		h.errors++
		h.mu.Unlock()
		h.logger.Error("cannot decode message", "offset", msg.Offset, "error", err)
		session.MarkMessage(msg, "")
		return
	}

	if h.callback != nil {
		if err := h.callback(domainMsg); err != nil {
			h.mu.Lock()
			h.errors++
			h.mu.Unlock()
			h.logger.Error("message handler failed", "offset", msg.Offset, "error", err)
		} else {
			h.mu.Lock()
			h.processed++
			h.mu.Unlock()
		}
	}
	session.MarkMessage(msg, "")
}

func NewConsumerRepository(brokers []string, version sarama.KafkaVersion, cfg *config.ConsumerConfig,
	logger *slog.Logger) (*ConsumerRepository, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = version

	switch cfg.InitialOffset {
	case "oldest":
		saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	default:
		saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	saramaCfg.Consumer.Offsets.AutoCommit.Enable = cfg.AutoCommit
	if cfg.AutoCommit {
		saramaCfg.Consumer.Offsets.AutoCommit.Interval = config.GetDuration(cfg.AutoCommitInterval, "s")
	}

	saramaCfg.Consumer.MaxWaitTime = config.GetDuration(cfg.FetchWaitMax, "ms")
	saramaCfg.Consumer.Fetch.Min = cfg.FetchMin
	saramaCfg.Consumer.Fetch.Default = cfg.FetchDefault
	saramaCfg.Consumer.Group.Session.Timeout = config.GetDuration(cfg.SessionTimeout, "s")
	saramaCfg.Consumer.Group.Heartbeat.Interval = config.GetDuration(cfg.HeartbeatInterval, "s")

	switch cfg.RebalanceStrategy {
	case "roundrobin":
		saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	case "sticky":
		saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	default:
		saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	}

	saramaCfg.Consumer.Return.Errors = true

	group, err := sarama.NewConsumerGroup(brokers, cfg.GroupID, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("create consumer group: %w", err)
	}

	return &ConsumerRepository{group: group, topics: cfg.Topics, handler: &groupHandler{logger: logger}, logger: logger}, nil
}

func (c *ConsumerRepository) ConsumeMessages(ctx context.Context, callback func(*domain.Message) error) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return domain.ErrKafkaConsumerClosed
	}
	c.mu.RUnlock()

	c.handler.callback = callback

	go func() {
		for err := range c.group.Errors() {
			c.logger.Error("consumer group error", "error", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := c.group.Consume(ctx, c.topics, c.handler); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (c *ConsumerRepository) GetMetrics() (int64, int64) {
	c.handler.mu.Lock()
	defer c.handler.mu.Unlock()
	return c.handler.processed, c.handler.errors
}

func (c *ConsumerRepository) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	return c.group.Close()
}
