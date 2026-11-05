package domain_test

import (
	"testing"

	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/domain"
)

func TestNewMessage(t *testing.T) {
	if _, err := domain.NewMessage("", "content"); err != domain.ErrEmptyMessageID {
		t.Fatalf("empty id: got %v", err)
	}
	if _, err := domain.NewMessage("id", ""); err != domain.ErrEmptyMessageContent {
		t.Fatalf("empty content: got %v", err)
	}

	msg, err := domain.NewMessage("msg-1", "hello")
	if err != nil {
		t.Fatalf("valid message: %v", err)
	}
	if err := msg.Validate(); err != nil {
		t.Fatalf("a new message must be valid: %v", err)
	}
}

func TestValidate_RejectsMissingTimestamp(t *testing.T) {
	msg := &domain.Message{ID: "msg-1", Content: "hello"}
	if err := msg.Validate(); err != domain.ErrInvalidTimestamp {
		t.Fatalf("got %v, want ErrInvalidTimestamp", err)
	}
}
