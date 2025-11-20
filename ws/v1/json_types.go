package v1

import (
	"encoding/base64"
)

// JSON structures based on swagger schema

// Header represents a header key-value pair
type Header struct {
	Key   string `json:"key"`   // base64 encoded
	Value string `json:"value"` // base64 encoded
}

// InitRequest represents the init request
type InitRequest struct {
	ConfigOverrides map[string]string `json:"configOverrides,omitempty"`
}

// InitResponse represents the init response
type InitResponse struct {
	Error string `json:"error,omitempty"`
}

// ProduceRequest represents a produce request
type ProduceRequest struct {
	CorrelationID int64  `json:"correlationId"`
	Topic         string `json:"topic"`
	Message       string `json:"message"` // base64 encoded
}

// ProduceResponse represents a produce response
type ProduceResponse struct {
	CorrelationID int64  `json:"correlationId"`
	Error         string `json:"error,omitempty"`
}

// HProduceRequest represents a produce request with headers
type HProduceRequest struct {
	CorrelationID int64    `json:"correlationId"`
	Topic         string   `json:"topic"`
	Headers       []Header `json:"headers,omitempty"`
	Message       string   `json:"message"` // base64 encoded
}

// HProduceResponse represents a produce response with headers
type HProduceResponse struct {
	CorrelationID int64  `json:"correlationId"`
	Error         string `json:"error,omitempty"`
}

// SubscribeRequest represents a subscribe request
type SubscribeRequest struct {
	CorrelationID int64  `json:"correlationId"`
	Topic         string `json:"topic"`
	AutoCommit    bool   `json:"autoCommit"`
}

// SubscribeResponse represents a subscribe response
type SubscribeResponse struct {
	CorrelationID  int64  `json:"correlationId"`
	Error          string `json:"error,omitempty"`
	SubscriptionID int64  `json:"subscriptionId,omitempty"`
}

// HSubscribeRequest represents a subscribe request with headers
type HSubscribeRequest struct {
	CorrelationID int64  `json:"correlationId"`
	Topic         string `json:"topic"`
	AutoCommit    bool   `json:"autoCommit"`
}

// HSubscribeResponse represents a subscribe response with headers
type HSubscribeResponse struct {
	CorrelationID  int64  `json:"correlationId"`
	Error          string `json:"error,omitempty"`
	SubscriptionID int64  `json:"subscriptionId,omitempty"`
}

// UnsubscribeRequest represents an unsubscribe request
type UnsubscribeRequest struct {
	CorrelationID  int64 `json:"correlationId"`
	SubscriptionID int64 `json:"subscriptionId"`
}

// UnsubscribeResponse represents an unsubscribe response
type UnsubscribeResponse struct {
	CorrelationID int64  `json:"correlationId"`
	Error         string `json:"error,omitempty"`
}

// Message represents a message delivery
type Message struct {
	SubscriptionID int64  `json:"subscriptionId"`
	MessageID      string `json:"messageId"` // base64 encoded
	Payload        string `json:"payload"`   // base64 encoded
}

// HMessage represents a message delivery with headers
type HMessage struct {
	SubscriptionID int64    `json:"subscriptionId"`
	MessageID      string   `json:"messageId"` // base64 encoded
	Payload        string   `json:"payload"`   // base64 encoded
	Headers        []Header `json:"headers,omitempty"`
}

// FetchRequest represents a fetch request
type FetchRequest struct {
	CorrelationID int64  `json:"correlationId"`
	Topic         string `json:"topic"`
	AutoCommit    bool   `json:"autoCommit"`
	BatchSize     int64  `json:"batchSize"`
}

// FetchMessage represents a message returned by Fetch
type FetchMessage struct {
	MessageID string `json:"messageId"` // base64 encoded
	Payload   string `json:"payload"`   // base64 encoded
}

// FetchResponse represents a fetch response
type FetchResponse struct {
	CorrelationID  int64          `json:"correlationId"`
	Error          string         `json:"error,omitempty"`
	SubscriptionID int64          `json:"subscriptionId,omitempty"`
	Messages       []FetchMessage `json:"messages,omitempty"`
}

// HFetchRequest represents a fetch request with headers
type HFetchRequest struct {
	CorrelationID int64  `json:"correlationId"`
	Topic         string `json:"topic"`
	AutoCommit    bool   `json:"autoCommit"`
	BatchSize     int64  `json:"batchSize"`
}

// HFetchMessage represents a message with headers returned by HFetch
type HFetchMessage struct {
	MessageID string   `json:"messageId"` // base64 encoded
	Payload   string   `json:"payload"`   // base64 encoded
	Headers   []Header `json:"headers,omitempty"`
}

// HFetchResponse represents a fetch response with headers
type HFetchResponse struct {
	CorrelationID  int64           `json:"correlationId"`
	Error          string          `json:"error,omitempty"`
	SubscriptionID int64           `json:"subscriptionId,omitempty"`
	Messages       []HFetchMessage `json:"messages,omitempty"`
}

// AckRequest represents an ack request
type AckRequest struct {
	CorrelationID  int64    `json:"correlationId"`
	SubscriptionID int64    `json:"subscriptionId"`
	MessageIDs     []string `json:"messageIds"` // base64 encoded
}

// AckMessageResult represents ack result for individual message
type AckMessageResult struct {
	MessageID string `json:"messageId"` // base64 encoded
	Error     string `json:"error,omitempty"`
}

// AckResponse represents an ack response
type AckResponse struct {
	CorrelationID int64              `json:"correlationId"`
	Error         string             `json:"error,omitempty"`
	Results       []AckMessageResult `json:"results,omitempty"`
}

// NackRequest represents a nack request
type NackRequest struct {
	CorrelationID  int64    `json:"correlationId"`
	SubscriptionID int64    `json:"subscriptionId"`
	MessageIDs     []string `json:"messageIds"` // base64 encoded
}

// NackMessageResult represents nack result for individual message
type NackMessageResult struct {
	MessageID string `json:"messageId"` // base64 encoded
	Error     string `json:"error,omitempty"`
}

// NackResponse represents a nack response
type NackResponse struct {
	CorrelationID int64               `json:"correlationId"`
	Error         string              `json:"error,omitempty"`
	Results       []NackMessageResult `json:"results,omitempty"`
}

// BeginTxRequest represents a begin transaction request
type BeginTxRequest struct {
	CorrelationID int64 `json:"correlationId"`
}

// BeginTxResponse represents a begin transaction response
type BeginTxResponse struct {
	CorrelationID int64  `json:"correlationId"`
	Error         string `json:"error,omitempty"`
}

// CommitTxRequest represents a commit transaction request
type CommitTxRequest struct {
	CorrelationID int64 `json:"correlationId"`
}

// CommitTxResponse represents a commit transaction response
type CommitTxResponse struct {
	CorrelationID int64  `json:"correlationId"`
	Error         string `json:"error,omitempty"`
}

// RollbackTxRequest represents a rollback transaction request
type RollbackTxRequest struct {
	CorrelationID int64 `json:"correlationId"`
}

// RollbackTxResponse represents a rollback transaction response
type RollbackTxResponse struct {
	CorrelationID int64  `json:"correlationId"`
	Error         string `json:"error,omitempty"`
}

// FujinRequest represents the union type for all requests
type FujinRequest struct {
	Init        *InitRequest        `json:"init,omitempty"`
	Produce     *ProduceRequest     `json:"produce,omitempty"`
	Hproduce    *HProduceRequest    `json:"hproduce,omitempty"`
	Subscribe   *SubscribeRequest   `json:"subscribe,omitempty"`
	Hsubscribe  *HSubscribeRequest  `json:"hsubscribe,omitempty"`
	Unsubscribe *UnsubscribeRequest `json:"unsubscribe,omitempty"`
	Fetch       *FetchRequest       `json:"fetch,omitempty"`
	Hfetch      *HFetchRequest      `json:"hfetch,omitempty"`
	Ack         *AckRequest         `json:"ack,omitempty"`
	Nack        *NackRequest        `json:"nack,omitempty"`
	BeginTx     *BeginTxRequest     `json:"beginTx,omitempty"`
	CommitTx    *CommitTxRequest    `json:"commitTx,omitempty"`
	RollbackTx  *RollbackTxRequest  `json:"rollbackTx,omitempty"`
}

// FujinResponse represents the union type for all responses
type FujinResponse struct {
	Init        *InitResponse        `json:"init,omitempty"`
	Produce     *ProduceResponse     `json:"produce,omitempty"`
	Hproduce    *HProduceResponse    `json:"hproduce,omitempty"`
	Subscribe   *SubscribeResponse   `json:"subscribe,omitempty"`
	Hsubscribe  *HSubscribeResponse  `json:"hsubscribe,omitempty"`
	Unsubscribe *UnsubscribeResponse `json:"unsubscribe,omitempty"`
	Message     *Message             `json:"message,omitempty"`
	Hmessage    *HMessage            `json:"hmessage,omitempty"`
	Fetch       *FetchResponse       `json:"fetch,omitempty"`
	Hfetch      *HFetchResponse      `json:"hfetch,omitempty"`
	Ack         *AckResponse         `json:"ack,omitempty"`
	Nack        *NackResponse        `json:"nack,omitempty"`
	BeginTx     *BeginTxResponse     `json:"beginTx,omitempty"`
	CommitTx    *CommitTxResponse    `json:"commitTx,omitempty"`
	RollbackTx  *RollbackTxResponse  `json:"rollbackTx,omitempty"`
}

type GRPCError struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
	Details []any  `json:"details,omitempty"`
}

type WrappedGRPCResponse struct {
	Result *FujinResponse `json:"result,omitempty"`
	Error  *GRPCError     `json:"error,omitempty"`
}

// Helper functions for base64 encoding/decoding

// encodeBase64 encodes bytes to base64 string
func encodeBase64(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

// decodeBase64 decodes base64 string to bytes
func decodeBase64(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(s)
}

// Helper functions to convert between JSON and protobuf-like structures

// headersToV1Headers converts map[string]string to []V1Header
func headersToV1Headers(headers map[string]string) []Header {
	if len(headers) == 0 {
		return nil
	}
	result := make([]Header, 0, len(headers))
	for k, v := range headers {
		result = append(result, Header{
			Key:   encodeBase64([]byte(k)),
			Value: encodeBase64([]byte(v)),
		})
	}
	return result
}

// v1HeadersToHeaders converts []V1Header to map[string]string
func v1HeadersToHeaders(v1Headers []Header) (map[string]string, error) {
	if len(v1Headers) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(v1Headers))
	for _, h := range v1Headers {
		key, err := decodeBase64(h.Key)
		if err != nil {
			return nil, err
		}
		value, err := decodeBase64(h.Value)
		if err != nil {
			return nil, err
		}
		result[string(key)] = string(value)
	}
	return result, nil
}
