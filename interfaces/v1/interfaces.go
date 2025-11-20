package v1

import (
	"github.com/fujin-io/fujin-go/config"
	"github.com/fujin-io/fujin-go/models"
)

// Conn represents a connection to Fujin server
type Conn interface {
	Init(configOverrides map[string]string) (Stream, error)
	InitWith(configOverrides map[string]string, cfg *config.StreamConfig) (Stream, error)
	Close() error
}

// Stream represents a stream for producing and consuming messages
type Stream interface {
	Produce(topic string, p []byte) error
	HProduce(topic string, p []byte, headers map[string]string) error
	BeginTx() error
	CommitTx() error
	RollbackTx() error
	Subscribe(topic string, autoCommit bool, handler func(msg models.Msg)) (uint32, error)
	HSubscribe(topic string, autoCommit bool, handler func(msg models.Msg)) (uint32, error)
	Fetch(topic string, autoCommit bool, batchSize uint32) (models.FetchResult, error)
	HFetch(topic string, autoCommit bool, batchSize uint32) (models.FetchResult, error)
	Unsubscribe(subscriptionID uint32) error
	Ack(subscriptionID uint32, messageIDs ...[]byte) (models.AckResult, error)
	Nack(subscriptionID uint32, messageIDs ...[]byte) (models.NackResult, error)
	Close() error
}

// Subscription represents a subscription to a topic
type Subscription interface {
	Close() error
}
