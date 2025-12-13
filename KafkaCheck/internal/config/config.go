package config

import (
	"fmt"
	"os"
	"time"

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
	FetchMin           int      `yaml:"fetch_min"`
	FetchDefault       int      `yaml:"fetch_default"`
	SessionTimeout     int      `yaml:"session_timeout"`
	HeartbeatInterval  int      `yaml:"heartbeat_interval"`
	RebalanceStrategy  string   `yaml:"rebalance_strategy"`
}

type AppConfig struct {
	MessagesCount           int `yaml:"messages_count"`
	SendInterval            int `yaml:"send_interval"`
	ShutdownTimeout         int `yaml:"shutdown_timeout"`
	KafkaReadyTimeout       int `yaml:"kafka_ready_timeout"`
	KafkaReadyCheckInterval int `yaml:"kafka_ready_check_interval"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
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

func (c *Config) validate() error {
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers is required")
	}
	if c.Kafka.Producer.Topic == "" {
		return fmt.Errorf("kafka.producer.topic is required")
	}
	if len(c.Kafka.Consumer.Topics) == 0 {
		return fmt.Errorf("kafka.consumer.topics is required")
	}
	if c.Kafka.Consumer.GroupID == "" {
		return fmt.Errorf("kafka.consumer.group_id is required")
	}
	return nil
}

func (c *Config) GetShutdownTimeout() time.Duration {
	return time.Duration(c.App.ShutdownTimeout) * time.Second
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

/* GetDuration конвертирует число в time.Duration
   unit: "ms" | "s" | "m" | "h" */
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
