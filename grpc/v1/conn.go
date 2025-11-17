package v1

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin-go/config"
	pb "github.com/fujin-io/fujin/public/proto/grpc/v1"
	"google.golang.org/grpc"
)

// conn implements the Conn interface
type conn struct {
	addr     string
	grpcConn *grpc.ClientConn
	client   pb.FujinServiceClient
	logger   *slog.Logger
	mu       sync.RWMutex
	closed   bool
}

// Dial creates a new gRPC connection
func Dial(addr string, logger *slog.Logger, opts ...grpc.DialOption) (Conn, error) {
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
	c.client = pb.NewFujinServiceClient(grpcConn)

	c.logger.Info("connected to server", "address", addr)
	return c, nil
}

// Init creates a new stream with the given ID
func (c *conn) Init(configOverrides map[string]string) (Stream, error) {
	return c.InitWith(configOverrides, nil)
}

// ConnectWith creates a new stream with the given ID and config
func (c *conn) InitWith(configOverrides map[string]string, cfg *config.StreamConfig) (Stream, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, fmt.Errorf("connection is closed")
	}
	c.mu.RUnlock()

	stream, err := newStream(c.client, configOverrides, c.logger, cfg)
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
