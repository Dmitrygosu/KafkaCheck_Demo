package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"

	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/domain"
	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/pkg/api"
)

// fakeSession and fakeClaim implement only the methods ConsumeClaim uses; the
// embedded interfaces panic if anything else is called.
type fakeSession struct {
	sarama.ConsumerGroupSession
	ctx    context.Context
	marked []int64
}

func (s *fakeSession) Context() context.Context { return s.ctx }

func (s *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, _ string) {
	s.marked = append(s.marked, msg.Offset)
}

type fakeClaim struct {
	sarama.ConsumerGroupClaim
	messages chan *sarama.ConsumerMessage
}

func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

func encoded(t *testing.T, id string) []byte {
	t.Helper()
	msg, err := domain.NewMessage(id, "payload")
	if err != nil {
		t.Fatal(err)
	}
	data, err := api.SerializeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestGroupHandler_ConsumeClaim(t *testing.T) {
	var received []string
	h := &groupHandler{
		logger: discard,
		callback: func(m *domain.Message) error {
			received = append(received, m.ID)
			if m.ID == "bad-callback" {
				return errors.New("handler failed")
			}
			return nil
		},
	}

	claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 3)}
	claim.messages <- &sarama.ConsumerMessage{Offset: 0, Value: encoded(t, "ok")}
	claim.messages <- &sarama.ConsumerMessage{Offset: 1, Value: []byte("not protobuf")}
	claim.messages <- &sarama.ConsumerMessage{Offset: 2, Value: encoded(t, "bad-callback")}
	close(claim.messages)
	session := &fakeSession{ctx: context.Background()}

	if err := h.ConsumeClaim(session, claim); err != nil {
		t.Fatal(err)
	}

	if len(received) != 2 || received[0] != "ok" || received[1] != "bad-callback" {
		t.Fatalf("callback got %v", received)
	}
	if len(session.marked) != 3 {
		t.Fatalf("every message must be marked, got offsets %v", session.marked)
	}
	if h.processed != 1 || h.errors != 2 {
		t.Fatalf("metrics: processed=%d errors=%d, want 1 and 2", h.processed, h.errors)
	}
}

func TestGroupHandler_StopsWhenTheSessionEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h := &groupHandler{logger: discard}

	err := h.ConsumeClaim(&fakeSession{ctx: ctx}, &fakeClaim{messages: make(chan *sarama.ConsumerMessage)})
	if err != nil {
		t.Fatal(err)
	}
}
