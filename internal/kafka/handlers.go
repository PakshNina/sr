package kafka

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"
)

type simpleHandler struct {
	topicName   string
	messageType protoreflect.MessageType
}

// NewSimpleHandler returns simple handler
func NewSimpleHandler(topicNAme string, messageType protoreflect.MessageType) Handler {
	return &simpleHandler{
		topicName:   topicNAme,
		messageType: messageType,
	}
}

// HandleMessage handles message
func (h *simpleHandler) HandleMessage(ctx context.Context, message interface{}, offset int64) error {
	if message == nil {
		return fmt.Errorf("message is empty")
	}
	fmt.Printf("message %v with offset %d\n", message, offset)
	return nil
}

// TopicName returns topic name
func (h *simpleHandler) TopicName() string {
	return h.topicName
}

// MessageType returns message type
func (h *simpleHandler) MessageType() protoreflect.MessageType {
	return h.messageType
}
