package v1

import (
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fujin-io/fujin-go/fujin/v1/pool"
	"github.com/quic-go/quic-go"
)

const (
	maxBufSize    = 65536
	MaxVectorSize = 1024
)

type outbound struct {
	sync.Mutex
	v      net.Buffers   // vector
	wv     net.Buffers   // working vector
	wdl    time.Duration // write deadline
	c      *sync.Cond
	pb     int64        // pending bytes
	str    *quic.Stream // current quic stream
	closed atomic.Bool
	l      *slog.Logger
}

func newOutbound(
	str *quic.Stream, wdl time.Duration,
	l *slog.Logger) *outbound {
	o := &outbound{
		str: str,
		wdl: wdl,
		l:   l,
	}
	o.c = sync.NewCond(&(o.Mutex))

	return o
}

func (o *outbound) WriteLoop() {
	waitOK := false
	var closed bool

	for {
		o.Lock()
		if closed = o.IsClosed(); !closed {
			if waitOK && (o.pb == 0 || o.pb < maxBufSize) {
				o.c.Wait()
				closed = o.IsClosed()
			}
		}

		if closed {
			o.flushOutbound()
			o.Unlock()
			return
		}

		waitOK = o.flushOutbound()
		o.Unlock()
	}
}

func (o *outbound) EnqueueProto(proto []byte) {
	if o.IsClosed() {
		return
	}

	o.queueOutbound(proto)
	o.SignalFlush()
}

func (o *outbound) EnqueueProtoMulti(protos ...[]byte) {
	if o.IsClosed() {
		return
	}

	o.Lock()
	for _, proto := range protos {
		o.QueueOutboundNoLock(proto)
	}
	o.Unlock()
	o.SignalFlush()
}

func (o *outbound) flushOutbound() bool {
	defer func() {
		if o.IsClosed() {
			for i := range o.wv {
				pool.Put(o.wv[i])
			}
			o.wv = nil
		}
	}()

	if o.str == nil || o.pb == 0 {
		return true
	}

	detached, _ := o.getV()
	o.v = nil

	o.wv = append(o.wv, detached...)
	var _orig [MaxVectorSize][]byte
	orig := append(_orig[:0], o.wv...)

	startOfWv := o.wv[0:]

	start := time.Now()

	var n int64
	var wn int64
	var err error

	for len(o.wv) > 0 {
		wv := o.wv
		if len(wv) > MaxVectorSize {
			wv = wv[:MaxVectorSize]
		}
		consumed := len(wv)

		_ = o.str.SetWriteDeadline(start.Add(o.wdl))
		wn, err = wv.WriteTo(o.str)
		_ = o.str.SetWriteDeadline(time.Time{})

		n += wn
		o.wv = o.wv[consumed-len(wv):]
		if err != nil {
			o.l.Error("write buffers", "err", err)
			break
		}
	}

	for i := 0; i < len(orig)-len(o.wv); i++ {
		pool.Put(orig[i])
	}

	o.wv = append(startOfWv[:0], o.wv...)

	o.pb -= n
	if o.pb > 0 {
		o.SignalFlush()
	}

	return true
}

func (o *outbound) getV() (net.Buffers, int64) {
	return o.v, o.pb
}

func (o *outbound) SignalFlush() {
	o.c.Signal()
}

func (o *outbound) queueOutbound(data []byte) {
	if o.IsClosed() {
		return
	}

	o.Lock()
	defer o.Unlock()
	o.pb += int64(len(data))
	toBuffer := data
	if len(o.v) > 0 {
		last := &o.v[len(o.v)-1]
		if free := cap(*last) - len(*last); free > 0 {
			if l := len(toBuffer); l < free {
				free = l
			}
			*last = append(*last, toBuffer[:free]...)
			toBuffer = toBuffer[free:]
		}
	}

	for len(toBuffer) > 0 {
		new := pool.Get(len(toBuffer))
		n := copy(new[:cap(new)], toBuffer)
		o.v = append(o.v, new[:n])
		toBuffer = toBuffer[n:]
	}
}

func (o *outbound) QueueOutboundNoLock(data []byte) {
	o.pb += int64(len(data))
	toBuffer := data
	if len(o.v) > 0 {
		last := &o.v[len(o.v)-1]
		if free := cap(*last) - len(*last); free > 0 {
			if l := len(toBuffer); l < free {
				free = l
			}
			*last = append(*last, toBuffer[:free]...)
			toBuffer = toBuffer[free:]
		}
	}

	for len(toBuffer) > 0 {
		new := pool.Get(len(toBuffer))
		n := copy(new[:cap(new)], toBuffer)
		o.v = append(o.v, new[:n])
		toBuffer = toBuffer[n:]
	}
}

func (o *outbound) IsClosed() bool {
	return o.closed.Load()
}

func (o *outbound) Close() {
	o.closed.Store(true)
	o.c.Broadcast()
}

func (o *outbound) BroadcastCond() {
	o.c.Broadcast()
}
