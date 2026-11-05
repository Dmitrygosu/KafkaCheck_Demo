// Package config loads and validates config.yaml.
package config

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Logging LoggingConfig `yaml:"logging"`
	Kafka   KafkaConfig   `yaml:"kafka"`
	App     AppConfig     `yaml:"app"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type KafkaConfig struct {
	Brokers  []string       `yaml:"brokers"`
	Version  string         `yaml:"version"`
	Producer ProducerConfig `yaml:"producer"`
	Consumer ConsumerConfig `yaml:"consumer"`
}

type ProducerConfig struct {
	Topic           string `yaml:"topic"`
	RequiredAcks    string `yaml:"required_acks"`
	MaxMessageBytes int    `yaml:"max_message_bytes"`
	Timeout         int    `yaml:"timeout"`
	Compression     string `yaml:"compression"`
	FlushMessages   int    `yaml:"flush_messages"`
	FlushFrequency  int    `yaml:"flush_frequency"`
	RetryMax        int    `yaml:"retry_max"`
	RetryBackoff    int    `yaml:"retry_backoff"`
}

type ConsumerConfig struct {
	Topics             []string `yaml:"topics"`
	GroupID            string   `yaml:"group_id"`
	InitialOffset      string   `yaml:"initial_offset"`
	AutoCommit         bool     `yaml:"auto_commit"`
	AutoCommitInterval int      `yaml:"auto_commit_interval"`
	FetchWaitMax       int      `yaml:"fetch_wait_max"`
	FetchMin           int32    `yaml:"fetch_min"`
	FetchDefault       int32    `yaml:"fetch_default"`
	SessionTimeout     int      `yaml:"session_timeout"`
	HeartbeatInterval  int      `yaml:"heartbeat_interval"`
	RebalanceStrategy  string   `yaml:"rebalance_strategy"`
}

type AppConfig struct {
	MessagesCount           int `yaml:"messages_count"`
	SendInterval            int `yaml:"send_interval"`
	DemoDuration            int `yaml:"demo_duration"`
	KafkaReadyTimeout       int `yaml:"kafka_ready_timeout"`
	KafkaReadyCheckInterval int `yaml:"kafka_ready_check_interval"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path) //nolint:gosec // the path is chosen by the operator via the -config flag
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// validate reports every problem at once.
func (c *Config) validate() error {
	var errs []error
	if len(c.Kafka.Brokers) == 0 {
		errs = append(errs, errors.New("kafka.brokers is required"))
	}
	if _, err := c.Kafka.SaramaVersion(); err != nil {
		errs = append(errs, err)
	}
	if c.Kafka.Producer.Topic == "" {
		errs = append(errs, errors.New("kafka.producer.topic is required"))
	}
	if len(c.Kafka.Consumer.Topics) == 0 {
		errs = append(errs, errors.New("kafka.consumer.topics is required"))
	}
	if c.Kafka.Consumer.GroupID == "" {
		errs = append(errs, errors.New("kafka.consumer.group_id is required"))
	}
	if _, err := c.Logging.NewLogger(io.Discard); err != nil {
		errs = append(errs, err)
	}
	if c.App.MessagesCount < 0 || c.App.DemoDuration < 0 {
		errs = append(errs, errors.New("app.messages_count and app.demo_duration must not be negative"))
	}
	if c.App.KafkaReadyCheckInterval <= 0 {
		errs = append(errs, errors.New("app.kafka_ready_check_interval must be positive"))
	}
	return errors.Join(errs...)
}

// SaramaVersion parses kafka.version; an empty value means Sarama's default.
func (k *KafkaConfig) SaramaVersion() (sarama.KafkaVersion, error) {
	if k.Version == "" {
		return sarama.DefaultVersion, nil
	}
	v, err := sarama.ParseKafkaVersion(k.Version)
	if err != nil {
		return sarama.KafkaVersion{}, fmt.Errorf("kafka.version: %w", err)
	}
	return v, nil
}

// NewLogger builds a structured logger from logging.level (DEBUG, INFO, WARN,
// ERROR) and logging.format (text or json).
func (l *LoggingConfig) NewLogger(w io.Writer) (*slog.Logger, error) {
	var level slog.Level
	if l.Level != "" {
		if err := level.UnmarshalText([]byte(l.Level)); err != nil {
			return nil, fmt.Errorf("logging.level: %w", err)
		}
	}
	opts := &slog.HandlerOptions{Level: level}
	switch strings.ToLower(l.Format) {
	case "", "text":
		return slog.New(slog.NewTextHandler(w, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	default:
		return nil, fmt.Errorf("logging.format: unknown format %q (want text or json)", l.Format)
	}
}

func (c *Config) GetKafkaReadyTimeout() time.Duration {
	return time.Duration(c.App.KafkaReadyTimeout) * time.Second
}

func (c *Config) GetKafkaReadyCheckInterval() time.Duration {
	return time.Duration(c.App.KafkaReadyCheckInterval) * time.Second
}

func (c *Config) GetSendInterval() time.Duration {
	return time.Duration(c.App.SendInterval) * time.Millisecond
}

// GetDemoDuration is how long the demo runs; zero means until interrupted.
func (c *Config) GetDemoDuration() time.Duration {
	return time.Duration(c.App.DemoDuration) * time.Second
}

// GetDuration converts a number to a time.Duration; unit is "ms", "s", "m" or "h".
func GetDuration(value int, unit string) time.Duration {
	switch unit {
	case "ms":
		return time.Duration(value) * time.Millisecond
	case "s":
		return time.Duration(value) * time.Second
	case "m":
		return time.Duration(value) * time.Minute
	case "h":
		return time.Duration(value) * time.Hour
	default:
		return time.Duration(value) * time.Second
	}
}
