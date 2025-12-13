package api

import (
	"KafkaCheck/internal/domain"
	"fmt"

	"google.golang.org/protobuf/proto"
)

func SerializeMessage(msg *domain.Message) ([]byte, error) {
	protoMsg := &KafkaMessage{Id: msg.ID, Content: msg.Content, Timestamp: msg.Timestamp}
	data, err := proto.Marshal(protoMsg)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	return data, nil
}

func DeserializeMessage(data []byte) (*domain.Message, error) {
	var protoMsg KafkaMessage
	if err := proto.Unmarshal(data, &protoMsg); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	msg := &domain.Message{ID: protoMsg.Id, Content: protoMsg.Content, Timestamp: protoMsg.Timestamp}
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	return msg, nil
}
