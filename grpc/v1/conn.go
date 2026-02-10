package v1

import (
	"fmt"
	"log/slog"
	"sync"

	fujin_go "github.com/fujin-io/fujin-go"
	"google.golang.org/grpc"
)

// conn implements the Conn interface
type conn struct {
	addr     string
	grpcConn *grpc.ClientConn
	client   FujinServiceClient
	logger   *slog.Logger
	mu       sync.RWMutex
	closed   bool
}

// Dial creates a new gRPC connection
func Dial(addr string, logger *slog.Logger, opts ...grpc.DialOption) (fujin_go.Conn, error) {
	if logger == nil {
		logger = slog.Default()
	}

	c := &conn{
		addr:   addr,
		logger: logger.With("transport", "grpc"),
	}

	grpcConn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to server: %w", err)
	}

	c.grpcConn = grpcConn
	c.client = NewFujinServiceClient(grpcConn)

	c.logger.Info("connected to server", "address", addr)
	return c, nil
}

// Bind creates a new stream with the given ID
func (c *conn) Bind(connector string, opts ...fujin_go.BindOption) (fujin_go.Stream, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, fmt.Errorf("connection is closed")
	}
	c.mu.RUnlock()

	conf := &fujin_go.BindConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(conf)
		}
	}

	stream, err := newStream(c.client, connector, conf.Meta, conf.ConfigOverrides, c.logger, conf.Stream)
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

	if c.grpcConn != nil {
		if err := c.grpcConn.Close(); err != nil {
			c.logger.Error("close gRPC connection", "error", err)
			return err
		}
	}

	c.logger.Info("connection closed")
	return nil
}
