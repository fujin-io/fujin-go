// Package v1 implements the v1 API for Fujin GRPC Gateway transport (available at https://github.com/fujin-io/fujin-grpc-gateway)

package v1

import (
	"fmt"
	"log/slog"
	"net/url"
	"sync"

	"github.com/fujin-io/fujin-go/config"
	v1 "github.com/fujin-io/fujin-go/interfaces/v1"
	"github.com/gorilla/websocket"
)

// Ensure conn implements interfaces/v1.Conn
var _ v1.Conn = (*conn)(nil)

// conn implements the Conn interface for WebSocket transport
type conn struct {
	addr string
	url  *url.URL

	wsConns []*websocket.Conn

	logger *slog.Logger
	mu     sync.RWMutex
	closed bool
}

// Dial creates a new WebSocket connection
func Dial(addr string, logger *slog.Logger) (v1.Conn, error) {
	if logger == nil {
		logger = slog.Default()
	}

	// Parse URL and ensure it's a WebSocket URL
	u, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("parse address: %w", err)
	}

	// Ensure WebSocket scheme
	if u.Scheme != "ws" && u.Scheme != "wss" {
		if u.Scheme == "" {
			u.Scheme = "ws"
		} else {
			return nil, fmt.Errorf("invalid scheme: %s (expected ws or wss)", u.Scheme)
		}
	}

	c := &conn{
		addr:   addr,
		url:    u,
		logger: logger.With("transport", "websocket"),
	}

	c.logger.Info("connected to server", "address", addr)
	return c, nil
}

// Init creates a new stream with the given config overrides
func (c *conn) Init(configOverrides map[string]string) (v1.Stream, error) {
	return c.InitWith(configOverrides, nil)
}

// InitWith creates a new stream with the given config overrides and config
func (c *conn) InitWith(configOverrides map[string]string, cfg *config.StreamConfig) (v1.Stream, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, fmt.Errorf("connection is closed")
	}
	c.mu.RUnlock()

	wsConn, _, err := websocket.DefaultDialer.Dial(c.url.String()+"/stream", nil)
	if err != nil {
		return nil, fmt.Errorf("connect to server: %w", err)
	}

	stream, err := newStream(wsConn, configOverrides, c.logger, cfg)
	if err != nil {
		return nil, fmt.Errorf("create stream: %w", err)
	}

	return stream, nil
}

// Close closes the connection and all streams
func (c *conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	for _, wsConn := range c.wsConns {
		if err := wsConn.Close(); err != nil {
			c.logger.Error("close WebSocket connection", "error", err)
			return err
		}
	}

	c.logger.Info("connection closed")
	return nil
}
