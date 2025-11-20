package v1

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/fujin-io/fujin-go/config"
	v1 "github.com/fujin-io/fujin-go/interfaces/v1"
	v1proto "github.com/fujin-io/fujin/public/proto/fujin/v1"
	"github.com/quic-go/quic-go"
)

var (
	ReadBufferSize = 512

	DISCONNECT_REQ = []byte{byte(v1proto.OP_CODE_DISCONNECT)}
)

type conn struct {
	qconn *quic.Conn

	timeout time.Duration
	wdl     time.Duration
	closed  atomic.Bool

	l *slog.Logger
}

var _ v1.Conn = (*conn)(nil)

// Dial creates a new connection to Fujin server using the native QUIC protocol
func Dial(ctx context.Context, addr string, tlsConf *tls.Config, quicConf *quic.Config, logger *slog.Logger, opts ...Option) (v1.Conn, error) {
	if tlsConf != nil {
		tlsConf = tlsConf.Clone()
		tlsConf.NextProtos = []string{v1proto.Version}
	}

	quicConn, err := quic.DialAddr(ctx, addr, tlsConf, quicConf)
	if err != nil {
		return nil, fmt.Errorf("quic: dial addr: %w", err)
	}

	c := &conn{
		qconn:   quicConn,
		timeout: 10 * time.Second,
		wdl:     5 * time.Second,
		l:       logger,
	}

	if c.l == nil {
		c.l = slog.Default()
	}

	for _, opt := range opts {
		opt(c)
	}

	go func() {
		var pingBuf [1]byte

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if c.closed.Load() {
					return
				}
				str, err := quicConn.AcceptStream(ctx)
				if err != nil {
					c.l.Error("ping: accept stream", "err", err)
					continue
				}

				if err := handlePing(str, pingBuf[:]); err != nil {
					c.l.Error("ping: handle ping", "err", err)
					continue
				}
			}
		}
	}()

	return c, nil
}

func (c *conn) Init(configOverrides map[string]string) (v1.Stream, error) {
	return c.InitWith(configOverrides, nil)
}

func (c *conn) InitWith(configOverrides map[string]string, cfg *config.StreamConfig) (v1.Stream, error) {
	return newStream(c, configOverrides, cfg)
}

func (c *conn) Close() error {
	// disconnect and close all streams
	c.closed.Store(true)
	if err := c.qconn.CloseWithError(0x0, ""); err != nil {
		return fmt.Errorf("quic: close: %w", err)
	}
	return nil
}

func handlePing(str *quic.Stream, buf []byte) error {
	defer str.Close()

	_, err := str.Read(buf[:])
	if err == io.EOF {
		buf[0] = byte(v1proto.RESP_CODE_PONG)
		if _, err := str.Write(buf[:]); err != nil {
			return fmt.Errorf("ping: write pong: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("ping: read: %w", err)
	}

	return nil
}
