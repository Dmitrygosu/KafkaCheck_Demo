// Package app wires the producer and the consumer together and runs the demo:
// it waits for the brokers, sends a batch of messages and prints what the
// consumer group receives.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/config"
	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/domain"
	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/repository"
)

// Run starts the demo and returns an error if it cannot run.
func Run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("config load failed: %w", err)
	}
	logger, err := cfg.Logging.NewLogger(os.Stdout)
	if err != nil {
		return err
	}
	version, err := cfg.Kafka.SaramaVersion()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := waitForKafka(ctx, logger, cfg.Kafka.Brokers, cfg.GetKafkaReadyTimeout(), cfg.GetKafkaReadyCheckInterval()); err != nil {
		return fmt.Errorf("kafka not ready: %w", err)
	}
	logger.Info("kafka brokers are available")

	producer, err := repository.NewProducerRepository(cfg.Kafka.Brokers, version, &cfg.Kafka.Producer, logger)
	if err != nil {
		return fmt.Errorf("producer creation failed: %w", err)
	}
	defer closeAndLog(logger, "producer", producer.Close)

	consumer, err := repository.NewConsumerRepository(cfg.Kafka.Brokers, version, &cfg.Kafka.Consumer, logger)
	if err != nil {
		return fmt.Errorf("consumer creation failed: %w", err)
	}
	defer closeAndLog(logger, "consumer", consumer.Close)

	go runConsumer(ctx, logger, consumer)

	sendTestMessages(ctx, logger, producer, cfg.App.MessagesCount, cfg.GetSendInterval())

	logger.Info("waiting for messages, press Ctrl+C to exit")
	if d := cfg.GetDemoDuration(); d > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}
	<-ctx.Done()

	sent, sendErrors := producer.GetMetrics()
	received, receiveErrors := consumer.GetMetrics()
	logger.Info("demo finished",
		"delivered", sent, "delivery_errors", sendErrors, "received", received, "receive_errors", receiveErrors)
	return nil
}

func closeAndLog(logger *slog.Logger, name string, closeFn func() error) {
	if err := closeFn(); err != nil {
		logger.Error("close failed", "component", name, "error", err)
	}
}

func waitForKafka(ctx context.Context, logger *slog.Logger, brokers []string, timeout, interval time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for attempt := 1; ; attempt++ {
		if allBrokersReachable(ctx, logger, brokers) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("brokers %v are unreachable after %d attempts: %w", brokers, attempt, ctx.Err())
		case <-ticker.C:
		}
	}
}

func allBrokersReachable(ctx context.Context, logger *slog.Logger, brokers []string) bool {
	dialer := net.Dialer{Timeout: 2 * time.Second}
	ok := true
	for _, addr := range brokers {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			logger.Warn("broker unreachable", "broker", addr, "error", err)
			ok = false
			continue
		}
		_ = conn.Close()
	}
	return ok
}

func runConsumer(ctx context.Context, logger *slog.Logger, consumer *repository.ConsumerRepository) {
	err := consumer.ConsumeMessages(ctx, func(msg *domain.Message) error {
		logger.Info("received", "id", msg.ID, "content", msg.Content, "time", msg.GetFormattedTime())
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		logger.Error("consumer stopped", "error", err)
	}
}

func sendTestMessages(ctx context.Context, logger *slog.Logger, producer *repository.ProducerRepository,
	count int, interval time.Duration) {
	for i := 1; i <= count; i++ {
		msg, err := domain.NewMessage(fmt.Sprintf("msg-%d", i), fmt.Sprintf("Test message #%d", i))
		if err != nil {
			logger.Error("cannot create message", "error", err)
			continue
		}
		if err := producer.SendMessage(msg); err != nil {
			logger.Error("cannot send message", "id", msg.ID, "error", err)
			continue
		}
		logger.Info("sent", "id", msg.ID, "content", msg.Content)

		if i < count {
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}
}
