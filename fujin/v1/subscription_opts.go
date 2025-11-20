package v1

type SubscriptionOption func(s *subscription)

func WithAsync(flag bool) func(*subscription) {
	return func(s *subscription) {
		s.conf.Async = flag
	}
}

func WithMsgBufSize(size int) func(*subscription) {
	return func(s *subscription) {
		s.conf.MsgBufSize = size
	}
}

func WithPoolConfig(pool PoolConfig) func(*subscription) {
	return func(s *subscription) {
		s.conf.Pool = pool
	}
}

