package v1

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"

	fujinv1 "github.com/fujin-io/fujin-go/fujin/v1"
	grpcv1 "github.com/fujin-io/fujin-go/grpc/v1"
	v1 "github.com/fujin-io/fujin-go/interfaces/v1"
	wsv1 "github.com/fujin-io/fujin-go/ws/v1"
	"github.com/quic-go/quic-go"
	"google.golang.org/grpc"
)

// TransportType represents the type of transport to use
type TransportType string

const (
	TransportUnknown TransportType = "unknown"
	// TransportGRPC uses gRPC transport
	TransportGRPC TransportType = "grpc"
	// TransportWebSocket uses WebSocket transport
	TransportWebSocket TransportType = "ws"
	// TransportFujin uses native Fujin QUIC transport
	TransportFujin TransportType = "fujin"
)

// Option is a function that configures a Dial option
type Option func(*dialConfig)

type dialConfig struct {
	transport TransportType
	logger    *slog.Logger
	grpcOpts  []grpc.DialOption
	ctx       context.Context
	tlsConf   *tls.Config
	quicConf  *quic.Config
	fujinOpts []fujinv1.Option
}

// WithLogger sets the logger
func WithLogger(logger *slog.Logger) Option {
	return func(c *dialConfig) {
		c.logger = logger
	}
}

// WithGRPCOptions sets gRPC dial options (only used for gRPC transport)
func WithGRPCOptions(opts ...grpc.DialOption) Option {
	return func(c *dialConfig) {
		c.grpcOpts = opts
	}
}

// WithContext sets the context (only used for Fujin transport)
func WithContext(ctx context.Context) Option {
	return func(c *dialConfig) {
		c.ctx = ctx
	}
}

// WithTLSConfig sets the TLS configuration (only used for Fujin transport)
func WithTLSConfig(tlsConf *tls.Config) Option {
	return func(c *dialConfig) {
		c.tlsConf = tlsConf
	}
}

// WithQUICConfig sets the QUIC configuration (only used for Fujin transport)
func WithQUICConfig(quicConf *quic.Config) Option {
	return func(c *dialConfig) {
		c.quicConf = quicConf
	}
}

// WithFujinOptions sets Fujin-specific options (only used for Fujin transport)
func WithFujinOptions(opts ...fujinv1.Option) Option {
	return func(c *dialConfig) {
		c.fujinOpts = opts
	}
}

// Dial creates a new connection to Fujin server
func Dial(transport TransportType, addr string, opts ...Option) (v1.Conn, error) {
	cfg := &dialConfig{
		logger: slog.Default(),
		ctx:    context.Background(),
	}

	// Apply options
	for _, opt := range opts {
		opt(cfg)
	}

	cfg.transport = transport

	// Create connection based on transport type
	switch cfg.transport {
	case TransportGRPC:
		return grpcv1.Dial(addr, cfg.logger, cfg.grpcOpts...)
	case TransportWebSocket:
		return wsv1.Dial(addr, cfg.logger)
	case TransportFujin:
		// logger is passed directly to fujinv1.Dial, so fujinOpts can override it if needed
		return fujinv1.Dial(cfg.ctx, addr, cfg.tlsConf, cfg.quicConf, cfg.logger, cfg.fujinOpts...)
	default:
		return nil, fmt.Errorf("unknown transport type: %s", cfg.transport)
	}
}
