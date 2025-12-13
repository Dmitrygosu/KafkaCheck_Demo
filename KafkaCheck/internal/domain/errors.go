package domain

import "errors"

var (
	ErrEmptyMessageID      = errors.New("message id is empty")
	ErrEmptyMessageContent = errors.New("message content is empty")
	ErrInvalidTimestamp    = errors.New("invalid timestamp")
	ErrKafkaProducerClosed = errors.New("producer is closed")
	ErrKafkaConsumerClosed = errors.New("consumer is closed")
)
