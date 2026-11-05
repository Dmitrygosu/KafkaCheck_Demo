package api_test

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/domain"
	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/pkg/api"
)

func TestSerializeRoundTrip(t *testing.T) {
	msg, err := domain.NewMessage("msg-7", "привет, Kafka")
	if err != nil {
		t.Fatal(err)
	}

	data, err := api.SerializeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := api.DeserializeMessage(data)
	if err != nil {
		t.Fatal(err)
	}

	if *got != *msg {
		t.Fatalf("round trip changed the message: got %+v, want %+v", got, msg)
	}
}

func TestDeserializeRejectsBadInput(t *testing.T) {
	if _, err := api.DeserializeMessage([]byte{0xff, 0xff, 0xff}); err == nil {
		t.Fatal("garbage must not decode")
	}

	// Well-formed protobuf that violates the domain rules (no id).
	data, err := proto.Marshal(&api.KafkaMessage{Content: "orphan", Timestamp: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.DeserializeMessage(data); !errors.Is(err, domain.ErrEmptyMessageID) {
		t.Fatalf("got %v, want ErrEmptyMessageID", err)
	}
}
