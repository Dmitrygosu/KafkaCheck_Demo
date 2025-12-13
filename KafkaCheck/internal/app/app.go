/*
Package app содержит основную логику приложения.
Здесь происходит инициализация компонентов и управление жизненным циклом.
*/
package app

import (
	"KafkaCheck/internal/config"
	"KafkaCheck/internal/domain"
	"KafkaCheck/internal/repository"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Run запускает приложение и возвращает ошибку при сбое
func Run() error {
	log.Println("Starting KafkaCheck...")

	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		return fmt.Errorf("config load failed: %w", err)
	}
	log.Println("Configuration loaded")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setupSignalHandler(cancel)

	if err := waitForKafka(ctx, cfg.Kafka.Brokers, cfg.GetKafkaReadyTimeout(), cfg.GetKafkaReadyCheckInterval()); err != nil {
		return fmt.Errorf("kafka not ready: %w", err)
	}
	log.Println("Kafka brokers are available")

	producer, err := repository.NewProducerRepository(cfg.Kafka.Brokers, &cfg.Kafka.Producer)
	if err != nil {
		return fmt.Errorf("producer creation failed: %w", err)
	}
	defer producer.Close()
	log.Println("Producer initialized")

	consumer, err := repository.NewConsumerRepository(cfg.Kafka.Brokers, &cfg.Kafka.Consumer)
	if err != nil {
		return fmt.Errorf("consumer creation failed: %w", err)
	}
	defer consumer.Close()
	log.Println("Consumer initialized")

	go runConsumer(ctx, consumer)

	sendTestMessages(producer, cfg.App.MessagesCount, cfg.GetSendInterval())

	log.Println("Waiting for messages... (Ctrl+C to exit)")
	select {
	case <-ctx.Done():
		log.Println("Shutdown signal received")
	case <-time.After(15 * time.Second):
		log.Println("Demo timeout reached")
		cancel()
	}

	printMetrics(producer, consumer)
	log.Println("Application stopped")
	return nil
}

func setupSignalHandler(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v", sig)
		cancel()
	}()
}

func waitForKafka(ctx context.Context, brokers []string, timeout, interval time.Duration) error {
	log.Printf("Checking %d brokers: %v", len(brokers), brokers)
	
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	attempt := 1
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for brokers")
		case <-ticker.C:
			log.Printf("Attempt %d: checking brokers...", attempt)
			if checkAllBrokers(brokers) {
				return nil
			}
			attempt++
		}
	}
}

func checkAllBrokers(brokers []string) bool {
	allOK := true
	for _, addr := range brokers {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			log.Printf("  [FAIL] %s: %v", addr, err)
			allOK = false
			continue
		}
		conn.Close()
		log.Printf("  [OK] %s", addr)
	}
	return allOK
}

func runConsumer(ctx context.Context, consumer *repository.ConsumerRepository) {
	err := consumer.ConsumeMessages(ctx, func(msg *domain.Message) error {
		log.Printf("[RECV] id=%s content=%q time=%s", msg.ID, msg.Content, msg.GetFormattedTime())
		return nil
	})
	if err != nil && err != context.Canceled {
		log.Printf("Consumer error: %v", err)
	}
}

func sendTestMessages(producer *repository.ProducerRepository, count int, interval time.Duration) {
	log.Printf("Sending %d test messages...", count)
	for i := 1; i <= count; i++ {
		msg, err := domain.NewMessage(fmt.Sprintf("msg-%d", i), fmt.Sprintf("Test message #%d", i))
		if err != nil {
			log.Printf("Failed to create message: %v", err)
			continue
		}

		if err := producer.SendMessage(msg); err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}
		log.Printf("[SEND] id=%s content=%q", msg.ID, msg.Content)

		if i < count {
			time.Sleep(interval)
		}
	}
}

func printMetrics(producer *repository.ProducerRepository, consumer *repository.ConsumerRepository) {
	sent, sendErr := producer.GetMetrics()
	recv, recvErr := consumer.GetMetrics()
	log.Printf("Metrics: producer(sent=%d, errors=%d) consumer(received=%d, errors=%d)", sent, sendErr, recv, recvErr)
}
