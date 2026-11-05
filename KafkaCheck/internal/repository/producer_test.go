package repository

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"

	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/domain"
	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/pkg/api"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func mockConfig() *sarama.Config {
	cfg := mocks.NewTestConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	return cfg
}

func TestProducer_CountsDeliveriesAndFailures(t *testing.T) {
	mock := mocks.NewAsyncProducer(t, mockConfig())
	mock.ExpectInputWithMessageCheckerFunctionAndSucceed(func(m *sarama.ProducerMessage) error {
		if m.Topic != "demo" {
			return errors.New("wrong topic " + m.Topic)
		}
		value, err := m.Value.Encode()
		if err != nil {
			return err
		}
		decoded, err := api.DeserializeMessage(value)
		if err != nil {
			return err
		}
		if decoded.ID != "msg-1" {
			return errors.New("unexpected payload " + decoded.ID)
		}
		return nil
	})
	mock.ExpectInputAndFail(sarama.ErrOutOfBrokers)

	p := newProducerRepository(mock, "demo", discard)
	for _, id := range []string{"msg-1", "msg-2"} {
		msg, err := domain.NewMessage(id, "hello")
		if err != nil {
			t.Fatal(err)
		}
		if err := p.SendMessage(msg); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	delivered, failed := p.GetMetrics()
	if delivered != 1 || failed != 1 {
		t.Fatalf("metrics: delivered=%d failed=%d, want 1 and 1", delivered, failed)
	}
}

// chanProducer is a minimal AsyncProducer: it reports every input as
// delivered and closes its result channels once Close closes the input.
type chanProducer struct {
	sarama.AsyncProducer
	input     chan *sarama.ProducerMessage
	successes chan *sarama.ProducerMessage
	errors    chan *sarama.ProducerError
}

func newChanProducer() *chanProducer {
	p := &chanProducer{
		input:     make(chan *sarama.ProducerMessage),
		successes: make(chan *sarama.ProducerMessage),
		errors:    make(chan *sarama.ProducerError),
	}
	go func() {
		for msg := range p.input {
			p.successes <- msg
		}
		close(p.successes)
		close(p.errors)
	}()
	return p
}

func (p *chanProducer) Input() chan<- *sarama.ProducerMessage     { return p.input }
func (p *chanProducer) Successes() <-chan *sarama.ProducerMessage { return p.successes }
func (p *chanProducer) Errors() <-chan *sarama.ProducerError      { return p.errors }
func (p *chanProducer) Close() error                              { close(p.input); return nil }

// Close racing with many senders must never panic with "send on closed
// channel", and every accepted message must be reported (run with -race).
func TestProducer_CloseWhileSending(t *testing.T) {
	p := newProducerRepository(newChanProducer(), "demo", discard)

	const senders, perSender = 50, 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	start := make(chan struct{})
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < perSender; j++ {
				msg, _ := domain.NewMessage("id", "payload")
				err := p.SendMessage(msg)
				if err != nil && !errors.Is(err, domain.ErrKafkaProducerClosed) {
					t.Errorf("unexpected error: %v", err)
				}
				if err == nil {
					mu.Lock()
					accepted++
					mu.Unlock()
				}
				_, _ = p.GetMetrics()
			}
		}()
	}
	close(start)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	delivered, failed := p.GetMetrics()
	if int(delivered) != accepted || failed != 0 {
		t.Fatalf("delivered=%d failed=%d, want %d accepted messages delivered", delivered, failed, accepted)
	}
	msg, _ := domain.NewMessage("late", "payload")
	if err := p.SendMessage(msg); !errors.Is(err, domain.ErrKafkaProducerClosed) {
		t.Fatalf("send after Close: got %v, want ErrKafkaProducerClosed", err)
	}
}
