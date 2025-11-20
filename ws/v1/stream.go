package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fujin-io/fujin-go/config"
	"github.com/fujin-io/fujin-go/correlator"
	v1 "github.com/fujin-io/fujin-go/interfaces/v1"
	"github.com/fujin-io/fujin-go/models"
	"github.com/gorilla/websocket"
)

// Ensure wsStream implements interfaces/v1.Stream
var _ v1.Stream = (*wsStream)(nil)

const (
	defaultRPCWait = 10 * time.Second
)

// Ensure subscription implements interfaces/v1.Subscription
var _ v1.Subscription = (*subscription)(nil)

// wsStream implements the Stream interface for WebSocket transport
type wsStream struct {
	wsConn          *websocket.Conn
	configOverrides map[string]string
	logger          *slog.Logger

	connected atomic.Bool
	closed    atomic.Bool

	// config
	rpcWait         time.Duration
	backoffInitial  time.Duration
	backoffMax      time.Duration
	backoffMultiple float64

	produceCorrelator     *correlator.Correlator[error]
	subscribeCorrelator   *correlator.Correlator[uint32]
	hsubscribeCorrelator  *correlator.Correlator[uint32]
	fetchCorrelator       *correlator.Correlator[models.FetchResult]
	hfetchCorrelator      *correlator.Correlator[models.FetchResult]
	beginTxCorrelator     *correlator.Correlator[error]
	commitTxCorrelator    *correlator.Correlator[error]
	rollbackTxCorrelator  *correlator.Correlator[error]
	unsubscribeCorrelator *correlator.Correlator[error]
	ackCorrelator         *correlator.Correlator[models.AckResult]
	nackCorrelator        *correlator.Correlator[models.NackResult]

	subscriptions map[uint32]*subscription
	subsMu        sync.RWMutex

	responseCh   chan *FujinResponse
	responseDone chan struct{}

	ctx    context.Context
	cancel context.CancelFunc

	// WebSocket write mutex (WebSocket is not thread-safe for concurrent writes)
	writeMu sync.Mutex

	wg sync.WaitGroup
}

// subscription represents a subscription to a topic
type subscription struct {
	id          uint32
	topic       string
	handler     func(msg models.Msg)
	stream      *wsStream
	closed      atomic.Bool
	autoCommit  bool
	withHeaders bool
}

// Close closes the subscription
func (s *subscription) Close() error {
	if s.closed.Load() {
		return nil
	}

	s.closed.Store(true)

	s.stream.subsMu.Lock()
	delete(s.stream.subscriptions, s.id)
	s.stream.subsMu.Unlock()

	return nil
}

// newStream creates a new WebSocket stream
func newStream(wsConn *websocket.Conn, configOverrides map[string]string, logger *slog.Logger, cfg *config.StreamConfig) (*wsStream, error) {
	ctx, cancel := context.WithCancel(context.Background())

	s := &wsStream{
		wsConn:                wsConn,
		configOverrides:       configOverrides,
		logger:                logger,
		subscriptions:         make(map[uint32]*subscription),
		responseCh:            make(chan *FujinResponse, 1000),
		responseDone:          make(chan struct{}),
		ctx:                   ctx,
		cancel:                cancel,
		rpcWait:               defaultRPCWait,
		backoffInitial:        200 * time.Millisecond,
		backoffMax:            5 * time.Second,
		backoffMultiple:       2.0,
		produceCorrelator:     correlator.New[error](),
		subscribeCorrelator:   correlator.New[uint32](),
		hsubscribeCorrelator:  correlator.New[uint32](),
		fetchCorrelator:       correlator.New[models.FetchResult](),
		hfetchCorrelator:      correlator.New[models.FetchResult](),
		beginTxCorrelator:     correlator.New[error](),
		commitTxCorrelator:    correlator.New[error](),
		rollbackTxCorrelator:  correlator.New[error](),
		unsubscribeCorrelator: correlator.New[error](),
		ackCorrelator:         correlator.New[models.AckResult](),
		nackCorrelator:        correlator.New[models.NackResult](),
	}

	// apply config if provided
	if cfg != nil {
	}

	if err := s.start(); err != nil {
		cancel()
		return nil, err
	}

	return s, nil
}

// sendRequest sends a JSON request through WebSocket
func (s *wsStream) sendRequest(req *FujinRequest) error {
	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if err := s.wsConn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("write message: %w", err)
	}

	return nil
}

// start initializes the WebSocket stream and starts response handling
func (s *wsStream) start() error {
	s.wg.Add(1)
	go s.readResponses()

	// Send INIT request
	initReq := &FujinRequest{
		Init: &InitRequest{
			ConfigOverrides: s.configOverrides,
		},
	}

	if err := s.sendRequest(initReq); err != nil {
		return fmt.Errorf("failed to send init request: %w", err)
	}

	select {
	case resp := <-s.responseCh:
		if resp.Init != nil {
			if resp.Init.Error != "" {
				return fmt.Errorf("init error: %s", resp.Init.Error)
			}
			s.connected.Store(true)
			s.logger.Info("stream initialized")
			return nil
		}
		return fmt.Errorf("unexpected init response")
	case <-time.After(s.rpcWait):
		return fmt.Errorf("init timeout")
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// readResponses handles incoming responses from the server
func (s *wsStream) readResponses() {
	defer s.wg.Done()
	defer close(s.responseDone)

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			_, data, err := s.wsConn.ReadMessage()
			if err != nil {
				if s.ctx.Err() != nil || s.closed.Load() {
					return
				}
				// Check if it's a close error
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					s.logger.Info("websocket closed", "error", err)
					return
				}
				s.logger.Error("receive failed, attempting to reconnect", "error", err)
				if err := s.reconnectWithBackoff(); err != nil {
					s.logger.Error("reconnect failed, stopping stream", "error", err)
					return
				}
				// on successful reconnect continue loop to ReadMessage() on new connection
				continue
			}

			// Deserialize JSON message
			var resp WrappedGRPCResponse
			if err := json.Unmarshal(data, &resp); err != nil {
				s.logger.Error("failed to unmarshal response", "error", err)
				continue
			}

			if resp.Error != nil {
				s.logger.Error("receive from upstream failed, attempting to reconnect", "error", err)
				if err := s.reconnectWithBackoff(); err != nil {
					s.logger.Error("reconnect failed, stopping stream", "error", err)
					return
				}
				// on successful reconnect continue loop to ReadMessage() on new connection
				continue
			}

			s.routeResponse(resp.Result)
		}
	}
}

// reconnectWithBackoff attempts to re-establish the WebSocket connection and resubscribe
func (s *wsStream) reconnectWithBackoff() error {
	s.connected.Store(false)

	// Note: WebSocket reconnection requires a new connection from the parent conn
	// This is a simplified version - in practice, you might need to handle this differently
	// For now, we'll just return an error and let the caller handle reconnection at the conn level
	return fmt.Errorf("websocket connection lost, reconnection must be handled at connection level")
}

// routeResponse routes incoming responses to appropriate correlators
func (s *wsStream) routeResponse(resp *FujinResponse) {
	if resp.Init != nil {
		select {
		case s.responseCh <- resp:
		case <-s.ctx.Done():
			return
		}
		return
	}

	if resp.Produce != nil {
		if resp.Produce.Error != "" {
			s.produceCorrelator.Send(uint32(resp.Produce.CorrelationID), fmt.Errorf("produce error: %s", resp.Produce.Error))
		} else {
			s.produceCorrelator.Send(uint32(resp.Produce.CorrelationID), nil)
		}
		return
	}

	if resp.Hproduce != nil {
		if resp.Hproduce.Error != "" {
			s.produceCorrelator.Send(uint32(resp.Hproduce.CorrelationID), fmt.Errorf("hproduce error: %s", resp.Hproduce.Error))
		} else {
			s.produceCorrelator.Send(uint32(resp.Hproduce.CorrelationID), nil)
		}
		return
	}

	if resp.Subscribe != nil {
		if resp.Subscribe.Error != "" {
			s.subscribeCorrelator.Send(uint32(resp.Subscribe.CorrelationID), 0)
		} else {
			s.subscribeCorrelator.Send(uint32(resp.Subscribe.CorrelationID), uint32(resp.Subscribe.SubscriptionID))
		}
		return
	}

	if resp.Hsubscribe != nil {
		if resp.Hsubscribe.Error != "" {
			s.hsubscribeCorrelator.Send(uint32(resp.Hsubscribe.CorrelationID), 0)
		} else {
			s.hsubscribeCorrelator.Send(uint32(resp.Hsubscribe.CorrelationID), uint32(resp.Hsubscribe.SubscriptionID))
		}
		return
	}

	if resp.Fetch != nil {
		result := models.FetchResult{
			SubscriptionID: uint32(resp.Fetch.SubscriptionID),
			Messages:       make([]models.Msg, 0, len(resp.Fetch.Messages)),
		}
		if resp.Fetch.Error != "" {
			result.Error = fmt.Errorf("fetch error: %s", resp.Fetch.Error)
		}
		for _, msg := range resp.Fetch.Messages {
			messageID, _ := decodeBase64(msg.MessageID)
			payload, _ := decodeBase64(msg.Payload)
			result.Messages = append(result.Messages, models.Msg{
				SubscriptionID: uint32(resp.Fetch.SubscriptionID),
				MessageID:      messageID,
				Payload:        payload,
			})
		}
		s.fetchCorrelator.Send(uint32(resp.Fetch.CorrelationID), result)
		return
	}

	if resp.Hfetch != nil {
		result := models.FetchResult{
			SubscriptionID: uint32(resp.Hfetch.SubscriptionID),
			Messages:       make([]models.Msg, 0, len(resp.Hfetch.Messages)),
		}
		if resp.Hfetch.Error != "" {
			result.Error = fmt.Errorf("hfetch error: %s", resp.Hfetch.Error)
		}
		for _, msg := range resp.Hfetch.Messages {
			messageID, _ := decodeBase64(msg.MessageID)
			payload, _ := decodeBase64(msg.Payload)
			headers, _ := v1HeadersToHeaders(msg.Headers)
			result.Messages = append(result.Messages, models.Msg{
				SubscriptionID: uint32(resp.Hfetch.SubscriptionID),
				MessageID:      messageID,
				Payload:        payload,
				Headers:        headers,
			})
		}
		s.hfetchCorrelator.Send(uint32(resp.Hfetch.CorrelationID), result)
		return
	}

	if resp.BeginTx != nil {
		if resp.BeginTx.Error != "" {
			s.beginTxCorrelator.Send(uint32(resp.BeginTx.CorrelationID), fmt.Errorf("begin tx error: %s", resp.BeginTx.Error))
		} else {
			s.beginTxCorrelator.Send(uint32(resp.BeginTx.CorrelationID), nil)
		}
		return
	}

	if resp.CommitTx != nil {
		if resp.CommitTx.Error != "" {
			s.commitTxCorrelator.Send(uint32(resp.CommitTx.CorrelationID), fmt.Errorf("commit tx error: %s", resp.CommitTx.Error))
		} else {
			s.commitTxCorrelator.Send(uint32(resp.CommitTx.CorrelationID), nil)
		}
		return
	}

	if resp.RollbackTx != nil {
		if resp.RollbackTx.Error != "" {
			s.rollbackTxCorrelator.Send(uint32(resp.RollbackTx.CorrelationID), fmt.Errorf("rollback tx error: %s", resp.RollbackTx.Error))
		} else {
			s.rollbackTxCorrelator.Send(uint32(resp.RollbackTx.CorrelationID), nil)
		}
		return
	}

	if resp.Unsubscribe != nil {
		if resp.Unsubscribe.Error != "" {
			s.unsubscribeCorrelator.Send(uint32(resp.Unsubscribe.CorrelationID), fmt.Errorf("unsubscribe error: %s", resp.Unsubscribe.Error))
		} else {
			s.unsubscribeCorrelator.Send(uint32(resp.Unsubscribe.CorrelationID), nil)
		}
		return
	}

	if resp.Message != nil {
		s.subsMu.RLock()
		if sub, exists := s.subscriptions[uint32(resp.Message.SubscriptionID)]; exists {
			messageID, _ := decodeBase64(resp.Message.MessageID)
			payload, _ := decodeBase64(resp.Message.Payload)
			sub.handler(models.Msg{
				SubscriptionID: uint32(resp.Message.SubscriptionID),
				MessageID:      messageID,
				Payload:        payload,
				Headers:        nil,
			})
		}
		s.subsMu.RUnlock()
		return
	}

	if resp.Hmessage != nil {
		s.subsMu.RLock()
		if sub, exists := s.subscriptions[uint32(resp.Hmessage.SubscriptionID)]; exists {
			messageID, _ := decodeBase64(resp.Hmessage.MessageID)
			payload, _ := decodeBase64(resp.Hmessage.Payload)
			headers, _ := v1HeadersToHeaders(resp.Hmessage.Headers)
			sub.handler(models.Msg{
				SubscriptionID: uint32(resp.Hmessage.SubscriptionID),
				MessageID:      messageID,
				Payload:        payload,
				Headers:        headers,
			})
		}
		s.subsMu.RUnlock()
		return
	}

	if resp.Ack != nil {
		result := models.AckResult{}
		if resp.Ack.Error != "" {
			result.Error = fmt.Errorf("ack error: %s", resp.Ack.Error)
		}
		if len(resp.Ack.Results) > 0 {
			result.MessageResults = make([]models.AckMessageResult, len(resp.Ack.Results))
			for i, res := range resp.Ack.Results {
				messageID, _ := decodeBase64(res.MessageID)
				result.MessageResults[i] = models.AckMessageResult{
					MessageID: messageID,
				}
				if res.Error != "" {
					result.MessageResults[i].Error = fmt.Errorf("%s", res.Error)
				}
			}
		}
		s.ackCorrelator.Send(uint32(resp.Ack.CorrelationID), result)
		return
	}

	if resp.Nack != nil {
		result := models.NackResult{}
		if resp.Nack.Error != "" {
			result.Error = fmt.Errorf("nack error: %s", resp.Nack.Error)
		}
		if len(resp.Nack.Results) > 0 {
			result.MessageResults = make([]models.NackMessageResult, len(resp.Nack.Results))
			for i, res := range resp.Nack.Results {
				messageID, _ := decodeBase64(res.MessageID)
				result.MessageResults[i] = models.NackMessageResult{
					MessageID: messageID,
				}
				if res.Error != "" {
					result.MessageResults[i].Error = fmt.Errorf("%s", res.Error)
				}
			}
		}
		s.nackCorrelator.Send(uint32(resp.Nack.CorrelationID), result)
		return
	}
}

// Produce sends a message to the specified topic
func (s *wsStream) Produce(topic string, p []byte) error {
	return s.produce(topic, p)
}

// produce sends a message to a topic
func (s *wsStream) produce(topic string, p []byte) error {
	if !s.connected.Load() {
		return fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return fmt.Errorf("stream is closed")
	}

	ch := make(chan error, 1)
	correlationID := s.produceCorrelator.Next(ch)

	req := &FujinRequest{
		Produce: &ProduceRequest{
			CorrelationID: int64(correlationID),
			Topic:         topic,
			Message:       encodeBase64(p),
		},
	}

	if err := s.sendRequest(req); err != nil {
		s.produceCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send produce request: %w", err)
	}

	select {
	case err := <-ch:
		s.produceCorrelator.Delete(correlationID)
		return err
	case <-time.After(s.rpcWait):
		s.produceCorrelator.Delete(correlationID)
		return fmt.Errorf("produce timeout")
	case <-s.ctx.Done():
		s.produceCorrelator.Delete(correlationID)
		return s.ctx.Err()
	}
}

// HProduce sends a message with headers to the specified topic
func (s *wsStream) HProduce(topic string, p []byte, headers map[string]string) error {
	return s.hproduce(topic, p, headers)
}

// hproduce sends a message with headers to a topic
func (s *wsStream) hproduce(topic string, p []byte, headers map[string]string) error {
	if !s.connected.Load() {
		return fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return fmt.Errorf("stream is closed")
	}

	ch := make(chan error, 1)
	correlationID := s.produceCorrelator.Next(ch)

	req := &FujinRequest{
		Hproduce: &HProduceRequest{
			CorrelationID: int64(correlationID),
			Topic:         topic,
			Headers:       headersToV1Headers(headers),
			Message:       encodeBase64(p),
		},
	}

	if err := s.sendRequest(req); err != nil {
		s.produceCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send hproduce request: %w", err)
	}

	select {
	case err := <-ch:
		s.produceCorrelator.Delete(correlationID)
		return err
	case <-time.After(s.rpcWait):
		s.produceCorrelator.Delete(correlationID)
		return fmt.Errorf("hproduce timeout")
	case <-s.ctx.Done():
		s.produceCorrelator.Delete(correlationID)
		return s.ctx.Err()
	}
}

// Subscribe subscribes to a topic
func (s *wsStream) Subscribe(topic string, autoCommit bool, handler func(msg models.Msg)) (uint32, error) {
	return s.subscribe(topic, autoCommit, handler)
}

func (s *wsStream) subscribe(topic string, autoCommit bool, handler func(msg models.Msg)) (uint32, error) {
	if !s.connected.Load() {
		return 0, fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return 0, fmt.Errorf("stream is closed")
	}

	ch := make(chan uint32, 1)
	correlationID := s.subscribeCorrelator.Next(ch)

	req := &FujinRequest{
		Subscribe: &SubscribeRequest{
			CorrelationID: int64(correlationID),
			Topic:         topic,
			AutoCommit:    autoCommit,
		},
	}

	if err := s.sendRequest(req); err != nil {
		s.subscribeCorrelator.Delete(correlationID)
		return 0, fmt.Errorf("failed to send subscribe request: %w", err)
	}

	select {
	case subscriptionID := <-ch:
		s.subscribeCorrelator.Delete(correlationID)

		if subscriptionID == 0 {
			return 0, fmt.Errorf("subscribe failed")
		}

		sub := &subscription{
			id:          subscriptionID,
			topic:       topic,
			handler:     handler,
			stream:      s,
			autoCommit:  autoCommit,
			withHeaders: false,
		}

		s.subsMu.Lock()
		s.subscriptions[subscriptionID] = sub
		s.subsMu.Unlock()

		s.logger.Info("subscribed to topic", "topic", topic, "subscription_id", subscriptionID)
		return subscriptionID, nil
	case <-time.After(s.rpcWait):
		s.subscribeCorrelator.Delete(correlationID)
		return 0, fmt.Errorf("subscribe timeout")
	case <-s.ctx.Done():
		s.subscribeCorrelator.Delete(correlationID)
		return 0, s.ctx.Err()
	}
}

// HSubscribe subscribes to a topic with headers support
func (s *wsStream) HSubscribe(topic string, autoCommit bool, handler func(msg models.Msg)) (uint32, error) {
	return s.hsubscribe(topic, autoCommit, handler)
}

func (s *wsStream) hsubscribe(topic string, autoCommit bool, handler func(msg models.Msg)) (uint32, error) {
	if !s.connected.Load() {
		return 0, fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return 0, fmt.Errorf("stream is closed")
	}

	ch := make(chan uint32, 1)
	correlationID := s.hsubscribeCorrelator.Next(ch)

	req := &FujinRequest{
		Hsubscribe: &HSubscribeRequest{
			CorrelationID: int64(correlationID),
			Topic:         topic,
			AutoCommit:    autoCommit,
		},
	}

	if err := s.sendRequest(req); err != nil {
		s.hsubscribeCorrelator.Delete(correlationID)
		return 0, fmt.Errorf("failed to send hsubscribe request: %w", err)
	}

	select {
	case subscriptionID := <-ch:
		s.hsubscribeCorrelator.Delete(correlationID)

		if subscriptionID == 0 {
			return 0, fmt.Errorf("hsubscribe failed")
		}

		sub := &subscription{
			id:          subscriptionID,
			topic:       topic,
			handler:     handler,
			stream:      s,
			autoCommit:  autoCommit,
			withHeaders: true,
		}

		s.subsMu.Lock()
		s.subscriptions[subscriptionID] = sub
		s.subsMu.Unlock()

		s.logger.Info("hsubscribed to topic", "topic", topic, "subscription_id", subscriptionID)
		return subscriptionID, nil
	case <-time.After(s.rpcWait):
		s.hsubscribeCorrelator.Delete(correlationID)
		return 0, fmt.Errorf("hsubscribe timeout")
	case <-s.ctx.Done():
		s.hsubscribeCorrelator.Delete(correlationID)
		return 0, s.ctx.Err()
	}
}

// Fetch requests a batch of messages from a topic (pull-based)
func (s *wsStream) Fetch(topic string, autoCommit bool, batchSize uint32) (models.FetchResult, error) {
	return s.fetch(topic, autoCommit, batchSize)
}

// fetch sends a fetch request and waits for the response
func (s *wsStream) fetch(topic string, autoCommit bool, batchSize uint32) (models.FetchResult, error) {
	if !s.connected.Load() {
		return models.FetchResult{}, fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return models.FetchResult{}, fmt.Errorf("stream is closed")
	}

	ch := make(chan models.FetchResult, 1)
	correlationID := s.fetchCorrelator.Next(ch)

	req := &FujinRequest{
		Fetch: &FetchRequest{
			CorrelationID: int64(correlationID),
			Topic:         topic,
			AutoCommit:    autoCommit,
			BatchSize:     int64(batchSize),
		},
	}

	if err := s.sendRequest(req); err != nil {
		s.fetchCorrelator.Delete(correlationID)
		return models.FetchResult{}, fmt.Errorf("failed to send fetch request: %w", err)
	}

	select {
	case result := <-ch:
		s.fetchCorrelator.Delete(correlationID)
		return result, result.Error
	case <-time.After(s.rpcWait):
		s.fetchCorrelator.Delete(correlationID)
		return models.FetchResult{}, fmt.Errorf("fetch timeout")
	case <-s.ctx.Done():
		s.fetchCorrelator.Delete(correlationID)
		return models.FetchResult{}, s.ctx.Err()
	}
}

// HFetch requests a batch of messages with headers from a topic (pull-based)
func (s *wsStream) HFetch(topic string, autoCommit bool, batchSize uint32) (models.FetchResult, error) {
	return s.hfetch(topic, autoCommit, batchSize)
}

// hfetch sends an hfetch request and waits for the response
func (s *wsStream) hfetch(topic string, autoCommit bool, batchSize uint32) (models.FetchResult, error) {
	if !s.connected.Load() {
		return models.FetchResult{}, fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return models.FetchResult{}, fmt.Errorf("stream is closed")
	}

	ch := make(chan models.FetchResult, 1)
	correlationID := s.hfetchCorrelator.Next(ch)

	req := &FujinRequest{
		Hfetch: &HFetchRequest{
			CorrelationID: int64(correlationID),
			Topic:         topic,
			AutoCommit:    autoCommit,
			BatchSize:     int64(batchSize),
		},
	}

	if err := s.sendRequest(req); err != nil {
		s.hfetchCorrelator.Delete(correlationID)
		return models.FetchResult{}, fmt.Errorf("failed to send hfetch request: %w", err)
	}

	select {
	case result := <-ch:
		s.hfetchCorrelator.Delete(correlationID)
		return result, result.Error
	case <-time.After(s.rpcWait):
		s.hfetchCorrelator.Delete(correlationID)
		return models.FetchResult{}, fmt.Errorf("hfetch timeout")
	case <-s.ctx.Done():
		s.hfetchCorrelator.Delete(correlationID)
		return models.FetchResult{}, s.ctx.Err()
	}
}

// BeginTx begins a transaction
func (s *wsStream) BeginTx() error {
	if !s.connected.Load() {
		return fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return fmt.Errorf("stream is closed")
	}

	ch := make(chan error, 1)
	correlationID := s.beginTxCorrelator.Next(ch)

	req := &FujinRequest{
		BeginTx: &BeginTxRequest{
			CorrelationID: int64(correlationID),
		},
	}

	if err := s.sendRequest(req); err != nil {
		s.beginTxCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send begin tx request: %w", err)
	}

	select {
	case err := <-ch:
		s.beginTxCorrelator.Delete(correlationID)
		return err
	case <-time.After(s.rpcWait):
		s.beginTxCorrelator.Delete(correlationID)
		return fmt.Errorf("begin tx timeout")
	case <-s.ctx.Done():
		s.beginTxCorrelator.Delete(correlationID)
		return s.ctx.Err()
	}
}

// CommitTx commits the current transaction
func (s *wsStream) CommitTx() error {
	if !s.connected.Load() {
		return fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return fmt.Errorf("stream is closed")
	}

	ch := make(chan error, 1)
	correlationID := s.commitTxCorrelator.Next(ch)

	req := &FujinRequest{
		CommitTx: &CommitTxRequest{
			CorrelationID: int64(correlationID),
		},
	}

	if err := s.sendRequest(req); err != nil {
		s.commitTxCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send commit tx request: %w", err)
	}

	select {
	case err := <-ch:
		s.commitTxCorrelator.Delete(correlationID)
		return err
	case <-time.After(s.rpcWait):
		s.commitTxCorrelator.Delete(correlationID)
		return fmt.Errorf("commit tx timeout")
	case <-s.ctx.Done():
		s.commitTxCorrelator.Delete(correlationID)
		return s.ctx.Err()
	}
}

// RollbackTx rolls back the current transaction
func (s *wsStream) RollbackTx() error {
	if !s.connected.Load() {
		return fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return fmt.Errorf("stream is closed")
	}

	ch := make(chan error, 1)
	correlationID := s.rollbackTxCorrelator.Next(ch)

	req := &FujinRequest{
		RollbackTx: &RollbackTxRequest{
			CorrelationID: int64(correlationID),
		},
	}

	if err := s.sendRequest(req); err != nil {
		s.rollbackTxCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send rollback tx request: %w", err)
	}

	select {
	case err := <-ch:
		s.rollbackTxCorrelator.Delete(correlationID)
		return err
	case <-time.After(s.rpcWait):
		s.rollbackTxCorrelator.Delete(correlationID)
		return fmt.Errorf("rollback tx timeout")
	case <-s.ctx.Done():
		s.rollbackTxCorrelator.Delete(correlationID)
		return s.ctx.Err()
	}
}

// Close closes the stream
func (s *wsStream) Close() error {
	if s.closed.Load() {
		return nil
	}

	s.closed.Store(true)
	s.cancel()

	s.subsMu.Lock()
	for _, sub := range s.subscriptions {
		sub.closed.Store(true)
	}
	s.subscriptions = make(map[uint32]*subscription)
	s.subsMu.Unlock()

	if err := s.wsConn.Close(); err != nil {
		s.logger.Error("close ws connection", "error", err)
	}

	s.wg.Wait()

	s.logger.Info("stream closed")
	return nil
}

// Unsubscribe unsubscribes from a topic
func (s *wsStream) Unsubscribe(subscriptionID uint32) error {
	if !s.connected.Load() {
		return fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return fmt.Errorf("stream is closed")
	}

	ch := make(chan error, 1)
	correlationID := s.unsubscribeCorrelator.Next(ch)

	req := &FujinRequest{
		Unsubscribe: &UnsubscribeRequest{
			CorrelationID:  int64(correlationID),
			SubscriptionID: int64(subscriptionID),
		},
	}

	if err := s.sendRequest(req); err != nil {
		s.unsubscribeCorrelator.Delete(correlationID)
		return fmt.Errorf("failed to send unsubscribe request: %w", err)
	}

	select {
	case err := <-ch:
		s.unsubscribeCorrelator.Delete(correlationID)

		if err != nil {
			return err
		}

		s.subsMu.Lock()
		delete(s.subscriptions, subscriptionID)
		s.subsMu.Unlock()

		s.logger.Info("unsubscribed from topic", "subscription_id", subscriptionID)
		return nil
	case <-time.After(s.rpcWait):
		s.unsubscribeCorrelator.Delete(correlationID)
		return fmt.Errorf("unsubscribe timeout")
	case <-s.ctx.Done():
		s.unsubscribeCorrelator.Delete(correlationID)
		return s.ctx.Err()
	}
}

// Ack acknowledges one or more messages for a subscription
func (s *wsStream) Ack(subscriptionID uint32, messageIDs ...[]byte) (models.AckResult, error) {
	if !s.connected.Load() {
		return models.AckResult{}, fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return models.AckResult{}, fmt.Errorf("stream is closed")
	}

	if len(messageIDs) == 0 {
		return models.AckResult{}, fmt.Errorf("at least one message ID is required")
	}

	ch := make(chan models.AckResult, 1)
	correlationID := s.ackCorrelator.Next(ch)

	encodedMessageIDs := make([]string, len(messageIDs))
	for i, msgID := range messageIDs {
		encodedMessageIDs[i] = encodeBase64(msgID)
	}

	req := &FujinRequest{
		Ack: &AckRequest{
			CorrelationID:  int64(correlationID),
			MessageIDs:     encodedMessageIDs,
			SubscriptionID: int64(subscriptionID),
		},
	}

	if err := s.sendRequest(req); err != nil {
		s.ackCorrelator.Delete(correlationID)
		return models.AckResult{}, fmt.Errorf("failed to send ack request: %w", err)
	}

	select {
	case result := <-ch:
		s.ackCorrelator.Delete(correlationID)
		if result.Error != nil {
			return result, result.Error
		}
		s.logger.Debug("ack successful", "subscription_id", subscriptionID, "message_count", len(messageIDs))
		return result, nil
	case <-time.After(s.rpcWait):
		s.ackCorrelator.Delete(correlationID)
		return models.AckResult{}, fmt.Errorf("ack timeout")
	case <-s.ctx.Done():
		s.ackCorrelator.Delete(correlationID)
		return models.AckResult{}, s.ctx.Err()
	}
}

// Nack negatively acknowledges one or more messages for a subscription
func (s *wsStream) Nack(subscriptionID uint32, messageIDs ...[]byte) (models.NackResult, error) {
	if !s.connected.Load() {
		return models.NackResult{}, fmt.Errorf("stream not connected")
	}

	if s.closed.Load() {
		return models.NackResult{}, fmt.Errorf("stream is closed")
	}

	if len(messageIDs) == 0 {
		return models.NackResult{}, fmt.Errorf("at least one message ID is required")
	}

	ch := make(chan models.NackResult, 1)
	correlationID := s.nackCorrelator.Next(ch)

	encodedMessageIDs := make([]string, len(messageIDs))
	for i, msgID := range messageIDs {
		encodedMessageIDs[i] = encodeBase64(msgID)
	}

	req := &FujinRequest{
		Nack: &NackRequest{
			CorrelationID:  int64(correlationID),
			MessageIDs:     encodedMessageIDs,
			SubscriptionID: int64(subscriptionID),
		},
	}

	if err := s.sendRequest(req); err != nil {
		s.nackCorrelator.Delete(correlationID)
		return models.NackResult{}, fmt.Errorf("failed to send nack request: %w", err)
	}

	select {
	case result := <-ch:
		s.nackCorrelator.Delete(correlationID)
		if result.Error != nil {
			return result, result.Error
		}
		s.logger.Debug("nack successful", "subscription_id", subscriptionID, "message_count", len(messageIDs))
		return result, nil
	case <-time.After(s.rpcWait):
		s.nackCorrelator.Delete(correlationID)
		return models.NackResult{}, fmt.Errorf("nack timeout")
	case <-s.ctx.Done():
		s.nackCorrelator.Delete(correlationID)
		return models.NackResult{}, s.ctx.Err()
	}
}
