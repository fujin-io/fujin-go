package v1

import (
	"time"
)

type Option func(c *conn)

func WithTimeout(timeout time.Duration) Option {
	return func(c *conn) {
		c.timeout = timeout
	}
}

func WithWriteDeadline(wdl time.Duration) Option {
	return func(c *conn) {
		c.wdl = wdl
	}
}
