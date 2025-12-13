package domain

import (
	"fmt"
	"time"
)

type Message struct {
	ID        string
	Content   string
	Timestamp int64
}

func NewMessage(id, content string) (*Message, error) {
	if id == "" {
		return nil, ErrEmptyMessageID
	}
	if content == "" {
		return nil, ErrEmptyMessageContent
	}
	return &Message{ID: id, Content: content, Timestamp: time.Now().Unix()}, nil
}

func (m *Message) Validate() error {
	if m.ID == "" {
		return ErrEmptyMessageID
	}
	if m.Content == "" {
		return ErrEmptyMessageContent
	}
	if m.Timestamp <= 0 {
		return ErrInvalidTimestamp
	}
	return nil
}

func (m *Message) String() string {
	return fmt.Sprintf("[%s] %s", m.ID, m.Content)
}

func (m *Message) GetFormattedTime() string {
	return time.Unix(m.Timestamp, 0).Format("15:04:05")
}
