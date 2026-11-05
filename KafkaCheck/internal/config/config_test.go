package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/config"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad_ShippedConfigIsValid(t *testing.T) {
	cfg, err := config.Load("../../configs/config.yaml")
	if err != nil {
		t.Fatalf("configs/config.yaml must load: %v", err)
	}
	if _, err := cfg.Kafka.SaramaVersion(); err != nil {
		t.Fatal(err)
	}
}

func TestLoad_ReportsEveryProblem(t *testing.T) {
	path := writeConfig(t, `
logging:
  level: LOUD
  format: xml
kafka:
  version: "not-a-version"
app:
  kafka_ready_check_interval: 0
`)

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("an invalid config must be rejected")
	}
	for _, want := range []string{"kafka.brokers", "kafka.version", "kafka.producer.topic", "kafka.consumer.topics",
		"kafka.consumer.group_id", "logging.level", "kafka_ready_check_interval"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestNewLogger_Formats(t *testing.T) {
	for _, format := range []string{"", "text", "JSON"} {
		lc := config.LoggingConfig{Level: "debug", Format: format}
		if _, err := lc.NewLogger(os.Stderr); err != nil {
			t.Errorf("format %q: %v", format, err)
		}
	}
	lc := config.LoggingConfig{Format: "xml"}
	if _, err := lc.NewLogger(os.Stderr); err == nil {
		t.Error("unknown formats must be rejected")
	}
}
