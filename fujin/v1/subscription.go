package v1

import (
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/fujin-io/fujin-go/fujin/v1/pool"
	v1 "github.com/fujin-io/fujin-go/interfaces/v1"
	"github.com/fujin-io/fujin-go/models"
	v1proto "github.com/fujin-io/fujin/public/proto/fujin/v1"
)

var _ v1.Subscription = (*subscription)(nil)

type SubscriptionConfig struct {
	Async      bool
	MsgBufSize int
	Pool       PoolConfig
}

type subscription struct {
	conf       SubscriptionConfig
	autoCommit bool

	stream *stream

	id   byte
	h    func(models.Msg)
	msgs chan fujinMsg

	closed atomic.Bool
}

func (s *subscription) Close() error {
	if s.closed.Load() {
		return nil
	}

	s.closed.Store(true)

	ch := make(chan error)
	id := s.stream.cm.Next(ch)

	buf := pool.Get(6)
	buf = append(buf, byte(v1proto.OP_CODE_UNSUBSCRIBE))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = append(buf, s.id)

	s.stream.out.EnqueueProto(buf)
	pool.Put(buf)
	var err error
	select {
	case <-time.After(s.stream.conn.timeout):
		err = ErrTimeout
	case <-ch:
	}

	s.stream.subs.delete(s.id)
	close(s.msgs)
	return err
}

func (s *subscription) handle() {
	defer s.stream.wg.Done()
	for msg := range s.msgs {
		modelsMsg := msg.toModelsMsg()
		s.h(modelsMsg)
		if msg.id != nil {
			pool.Put(msg.id)
		}
		pool.Put(msg.Value)
	}
}
