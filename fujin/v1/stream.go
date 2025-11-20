package v1

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fujin-io/fujin-go/config"
	"github.com/fujin-io/fujin-go/correlator"
	"github.com/fujin-io/fujin-go/fujin/v1/pool"
	v1 "github.com/fujin-io/fujin-go/interfaces/v1"
	"github.com/fujin-io/fujin-go/models"
	v1proto "github.com/fujin-io/fujin/public/proto/fujin/v1"
	"github.com/quic-go/quic-go"
)

var (
	ErrStreamClosed = errors.New("stream closed")

	PONG_RESP = []byte{byte(v1proto.RESP_CODE_PONG)}
)

var _ v1.Stream = (*stream)(nil)

type stream struct {
	conn *conn

	ps         *parseState
	quicStream *quic.Stream
	out        *outbound

	subs *subscriptions

	cm  *correlator.Correlator[error]
	ac  *correlator.Correlator[AckResponse]
	fcm *fetchCorrelator
	sc  *correlator.Correlator[SubscribeResponse]

	closed       atomic.Bool
	disconnectCh chan struct{}
	wg           sync.WaitGroup

	l *slog.Logger
}

func newStream(c *conn, configOverrides map[string]string, _ *config.StreamConfig) (*stream, error) {
	quicStream, err := c.qconn.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("open stream: %w", err)
	}

	buflen := 1 + Uint16Len
	for k, v := range configOverrides {
		buflen += Uint32Len + len(k) + Uint32Len + len(v)
	}

	buf := pool.Get(buflen)
	defer pool.Put(buf)

	buf = append(buf, byte(v1proto.OP_CODE_INIT))
	buf = AppendFujinUint16StringArray(buf, configOverrides)

	if _, err := quicStream.Write(buf); err != nil {
		quicStream.Close()
		return nil, fmt.Errorf("write init: %w", err)
	}

	len := 1 + 1 + Uint32Len + 512
	initRespBuf := pool.Get(len)[:len] // max reasonable error size

	if _, err := io.ReadFull(quicStream, initRespBuf[:1]); err != nil {
		quicStream.Close()
		return nil, fmt.Errorf("read init response code: %w", err)
	}

	const RESP_CODE_INIT = byte(v1proto.RESP_CODE_INIT)
	if initRespBuf[0] != RESP_CODE_INIT {
		quicStream.Close()
		return nil, fmt.Errorf("unexpected init response code: %d, expected %d", initRespBuf[0], RESP_CODE_INIT)
	}

	if _, err := io.ReadFull(quicStream, initRespBuf[1:2]); err != nil {
		quicStream.Close()
		return nil, fmt.Errorf("read init error nullability: %w", err)
	}

	if initRespBuf[1] != 0 {
		if _, err := io.ReadFull(quicStream, initRespBuf[2:6]); err != nil {
			quicStream.Close()
			return nil, fmt.Errorf("read init error length: %w", err)
		}

		errorLen := binary.BigEndian.Uint32(initRespBuf[2:6])
		if errorLen == 0 {
			quicStream.Close()
			return nil, fmt.Errorf("invalid init error length: 0")
		}

		errorBuf := pool.Get(int(errorLen))
		defer pool.Put(errorBuf)

		if _, err := io.ReadFull(quicStream, errorBuf[:errorLen]); err != nil {
			quicStream.Close()
			return nil, fmt.Errorf("read init error message: %w", err)
		}

		quicStream.Close()
		return nil, fmt.Errorf("init error: %s", string(errorBuf[:errorLen]))
	}

	l := c.l.With("quic_stream_id", quicStream.StreamID())
	out := newOutbound(quicStream, c.wdl, l)

	s := &stream{
		conn:         c,
		out:          out,
		quicStream:   quicStream,
		ps:           &parseState{},
		subs:         newSubscriptions(),
		cm:           correlator.New[error](),
		ac:           correlator.New[AckResponse](),
		sc:           correlator.New[SubscribeResponse](),
		fcm:          newFetchCorrelator(),
		disconnectCh: make(chan struct{}),
		l:            l,
	}

	s.wg.Add(2)
	go s.readLoop()
	go func() {
		defer s.wg.Done()
		out.WriteLoop()
	}()

	return s, nil
}

func (s *stream) Produce(topic string, p []byte) error {
	if s.closed.Load() {
		return ErrStreamClosed
	}

	buf := pool.Get(len(topic) + len(p) + 13)
	ch := make(chan error, 1)
	id := s.cm.Next(ch)

	buf = append(buf, byte(v1proto.OP_CODE_PRODUCE))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(p)))
	buf = append(buf, p...)

	s.out.EnqueueProto(buf)

	ctx, cancel := context.WithTimeout(context.Background(), s.conn.timeout)

	select {
	case <-ctx.Done():
		cancel()
		s.cm.Delete(id)
		close(ch)
		pool.Put(buf)
		return ErrTimeout
	case err := <-ch:
		cancel()
		s.cm.Delete(id)
		close(ch)
		pool.Put(buf)
		return err
	}
}

func (s *stream) HProduce(topic string, p []byte, hs map[string]string) error {
	if len(hs) <= 0 {
		return s.Produce(topic, p)
	}

	if s.closed.Load() {
		return ErrStreamClosed
	}

	// Calculate buffer size: opcode(1) + cID(4) + topicLen(4) + topic + headerCount(2) + headers + payloadLen(4) + payload
	headersCount := len(hs) * 2
	headersSize := 0
	for k, v := range hs {
		headersSize += 4 + len(k) // key length + key
		headersSize += 4 + len(v) // value length + value
	}
	buf := pool.Get(1 + 4 + 4 + len(topic) + 2 + headersSize + 4 + len(p))
	ch := make(chan error, 1)
	id := s.cm.Next(ch)

	buf = append(buf, byte(v1proto.OP_CODE_HPRODUCE))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(headersCount))
	for k, v := range hs {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(p)))
	buf = append(buf, p...)

	s.out.EnqueueProto(buf)

	ctx, cancel := context.WithTimeout(context.Background(), s.conn.timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		s.cm.Delete(id)
		close(ch)
		pool.Put(buf)
		return ErrTimeout
	case err := <-ch:
		s.cm.Delete(id)
		close(ch)
		pool.Put(buf)
		return err
	}
}

func (s *stream) BeginTx() error {
	return s.sendTxCmd(byte(v1proto.OP_CODE_TX_BEGIN))
}

func (s *stream) CommitTx() error {
	return s.sendTxCmd(byte(v1proto.OP_CODE_TX_COMMIT))
}

func (s *stream) RollbackTx() error {
	return s.sendTxCmd(byte(v1proto.OP_CODE_TX_ROLLBACK))
}

func (s *stream) Fetch(
	topic string, autoCommit bool, batchSize uint32,
) (models.FetchResult, error) {
	if s.closed.Load() {
		return models.FetchResult{}, ErrStreamClosed
	}

	buf := pool.Get(9)
	defer pool.Put(buf)

	eCh := make(chan error, 1)

	id := s.fcm.next(eCh, autoCommit)
	defer s.fcm.delete(id)

	buf = append(buf, byte(v1proto.OP_CODE_FETCH))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = append(buf, boolToByte(autoCommit))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, []byte(topic)...)
	buf = binary.BigEndian.AppendUint32(buf, batchSize)

	s.out.EnqueueProto(buf)

	ctx, cancel := context.WithTimeout(context.Background(), s.conn.timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return models.FetchResult{}, ErrTimeout
	case err := <-eCh:
		if err != nil {
			return models.FetchResult{Error: err}, err
		}

		fujinMsgs := s.fcm.getMsgs(id)
		result := models.FetchResult{
			Messages: make([]models.Msg, 0, len(fujinMsgs)),
		}
		for _, msg := range fujinMsgs {
			result.Messages = append(result.Messages, msg.toModelsMsg())
		}
		return result, nil
	}
}

func (s *stream) HFetch(topic string, autoCommit bool, batchSize uint32) (models.FetchResult, error) {
	if s.closed.Load() {
		return models.FetchResult{}, ErrStreamClosed
	}

	buf := pool.Get(9)
	defer pool.Put(buf)

	eCh := make(chan error, 1)

	id := s.fcm.next(eCh, autoCommit)
	defer s.fcm.delete(id)

	buf = append(buf, byte(v1proto.OP_CODE_HFETCH))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = append(buf, boolToByte(autoCommit))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, []byte(topic)...)
	buf = binary.BigEndian.AppendUint32(buf, batchSize)

	s.out.EnqueueProto(buf)

	ctx, cancel := context.WithTimeout(context.Background(), s.conn.timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return models.FetchResult{}, ErrTimeout
	case err := <-eCh:
		if err != nil {
			return models.FetchResult{Error: err}, err
		}

		fujinMsgs := s.fcm.getMsgs(id)
		result := models.FetchResult{
			Messages: make([]models.Msg, 0, len(fujinMsgs)),
		}
		for _, msg := range fujinMsgs {
			result.Messages = append(result.Messages, msg.toModelsMsg())
		}
		return result, nil
	}
}

func (s *stream) Close() error {
	if s.closed.Load() {
		return nil
	}

	s.closed.Store(true)

	s.subs.close()

	s.out.EnqueueProto(DISCONNECT_REQ)
	select {
	case <-time.After(s.conn.timeout):
	case <-s.disconnectCh:
	}

	s.out.Close()
	s.quicStream.Close()

	s.wg.Wait()
	return nil
}

func (s *stream) Subscribe(topic string, autoCommit bool, handler func(msg models.Msg)) (uint32, error) {
	if topic == "" {
		return 0, ErrEmptyTopic
	}

	if s.closed.Load() {
		return 0, ErrStreamClosed
	}

	h := func(msg fujinMsg) {
		modelsMsg := msg.toModelsMsg()
		handler(modelsMsg)
		if msg.id != nil {
			pool.Put(msg.id)
		}
		pool.Put(msg.Value)
	}

	sub := &subscription{
		conf: SubscriptionConfig{
			MsgBufSize: 1024,
		},
		autoCommit: autoCommit,
		stream:     s,
		h:          func(m models.Msg) { h(fromModelsMsg(m)) },
	}

	sub.msgs = make(chan fujinMsg, sub.conf.MsgBufSize)

	ch := make(chan SubscribeResponse, 1)
	id := s.sc.Next(ch)

	buf := pool.Get(10 + len(topic))

	buf = append(buf, byte(v1proto.OP_CODE_SUBSCRIBE))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = append(buf, boolToByte(autoCommit))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)

	s.out.EnqueueProto(buf)
	pool.Put(buf)

	ctx, cancel := context.WithTimeout(context.Background(), s.conn.timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		s.sc.Delete(id)
		close(ch)
		return 0, ErrTimeout
	case resp := <-ch:
		s.sc.Delete(id)
		close(ch)
		if resp.Err != nil {
			return 0, resp.Err
		}
		sub.id = resp.SubID
		s.subs.add(sub)
		s.wg.Add(1)
		go sub.handle()
		return uint32(resp.SubID), nil
	}
}

func (s *stream) HSubscribe(topic string, autoCommit bool, handler func(msg models.Msg)) (uint32, error) {
	if topic == "" {
		return 0, ErrEmptyTopic
	}

	if s.closed.Load() {
		return 0, ErrStreamClosed
	}

	h := func(msg fujinMsg) {
		modelsMsg := msg.toModelsMsg()
		handler(modelsMsg)
		if msg.id != nil {
			pool.Put(msg.id)
		}
		pool.Put(msg.Value)
	}

	sub := &subscription{
		conf: SubscriptionConfig{
			MsgBufSize: 1024,
		},
		autoCommit: autoCommit,
		stream:     s,
		h:          func(m models.Msg) { h(fromModelsMsg(m)) },
	}

	sub.msgs = make(chan fujinMsg, sub.conf.MsgBufSize)

	ch := make(chan SubscribeResponse, 1)
	id := s.sc.Next(ch)

	buf := pool.Get(10 + len(topic))

	buf = append(buf, byte(v1proto.OP_CODE_HSUBSCRIBE))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = append(buf, boolToByte(autoCommit))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)

	s.out.EnqueueProto(buf)
	pool.Put(buf)

	ctx, cancel := context.WithTimeout(context.Background(), s.conn.timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		s.sc.Delete(id)
		close(ch)
		return 0, ErrTimeout
	case resp := <-ch:
		s.sc.Delete(id)
		close(ch)
		if resp.Err != nil {
			return 0, resp.Err
		}
		sub.id = resp.SubID
		s.subs.add(sub)
		s.wg.Add(1)
		go sub.handle()
		return uint32(resp.SubID), nil
	}
}

func (s *stream) Unsubscribe(subscriptionID uint32) error {
	if s.closed.Load() {
		return ErrStreamClosed
	}

	ch := make(chan error, 1)
	id := s.cm.Next(ch)
	defer s.cm.Delete(id)

	buf := pool.Get(6)
	buf = append(buf, byte(v1proto.OP_CODE_UNSUBSCRIBE))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = append(buf, byte(subscriptionID))

	s.out.EnqueueProto(buf)
	pool.Put(buf)

	ctx, cancel := context.WithTimeout(context.Background(), s.conn.timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return ErrTimeout
	case err := <-ch:
		if err != nil {
			return err
		}
		s.subs.delete(byte(subscriptionID))
		return nil
	}
}

func (s *stream) Ack(subscriptionID uint32, messageIDs ...[]byte) (models.AckResult, error) {
	if len(messageIDs) == 0 {
		return models.AckResult{}, fmt.Errorf("at least one message ID is required")
	}

	if s.closed.Load() {
		return models.AckResult{}, ErrStreamClosed
	}

	ch := make(chan AckResponse, 1)
	defer close(ch)

	correlationID := s.ac.Next(ch)
	defer s.ac.Delete(correlationID)

	buf := pool.Get(10 + len(messageIDs)*4)
	buf = append(buf, byte(v1proto.OP_CODE_ACK))
	buf = binary.BigEndian.AppendUint32(buf, correlationID)
	buf = append(buf, byte(subscriptionID))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(messageIDs)))

	var msgIDs [][]byte
	for _, msgID := range messageIDs {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(msgID)))
		msgIDs = append(msgIDs, msgID)
	}

	allProtos := append([][]byte{buf}, msgIDs...)
	s.out.EnqueueProtoMulti(allProtos...)
	pool.Put(buf)

	ctx, cancel := context.WithTimeout(context.Background(), s.conn.timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return models.AckResult{}, ErrTimeout
	case resp := <-ch:
		result := models.AckResult{}
		if resp.Err != nil {
			result.Error = resp.Err
		}
		if len(resp.AckMsgResponses) > 0 {
			result.MessageResults = make([]models.AckMessageResult, len(resp.AckMsgResponses))
			for i, ar := range resp.AckMsgResponses {
				result.MessageResults[i] = models.AckMessageResult{
					MessageID: ar.MsgID,
					Error:     ar.Err,
				}
			}
		}
		return result, resp.Err
	}
}

func (s *stream) Nack(subscriptionID uint32, messageIDs ...[]byte) (models.NackResult, error) {
	if len(messageIDs) == 0 {
		return models.NackResult{}, fmt.Errorf("at least one message ID is required")
	}

	if s.closed.Load() {
		return models.NackResult{}, ErrStreamClosed
	}

	ch := make(chan AckResponse, 1)
	defer close(ch)

	correlationID := s.ac.Next(ch)
	defer s.ac.Delete(correlationID)

	buf := pool.Get(10)
	buf = append(buf, byte(v1proto.OP_CODE_NACK))
	buf = binary.BigEndian.AppendUint32(buf, correlationID)
	buf = append(buf, byte(subscriptionID))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(messageIDs)))

	var msgIDs [][]byte
	for _, msgID := range messageIDs {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(msgID)))
		msgIDs = append(msgIDs, msgID)
	}

	allProtos := append([][]byte{buf}, msgIDs...)
	s.out.EnqueueProtoMulti(allProtos...)
	pool.Put(buf)

	ctx, cancel := context.WithTimeout(context.Background(), s.conn.timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return models.NackResult{}, ErrTimeout
	case resp := <-ch:
		result := models.NackResult{}
		if resp.Err != nil {
			result.Error = resp.Err
		}
		if len(resp.AckMsgResponses) > 0 {
			result.MessageResults = make([]models.NackMessageResult, len(resp.AckMsgResponses))
			for i, ar := range resp.AckMsgResponses {
				result.MessageResults[i] = models.NackMessageResult{
					MessageID: ar.MsgID,
					Error:     ar.Err,
				}
			}
		}
		return result, resp.Err
	}
}

func (s *stream) ack(subID byte, id []byte) (AckResponse, error) {
	return s.sendAckCmd(byte(v1proto.OP_CODE_ACK), subID, id)
}

func (s *stream) nack(subID byte, id []byte) (AckResponse, error) {
	return s.sendAckCmd(byte(v1proto.OP_CODE_NACK), subID, id)
}

func (s *stream) sendAckCmd(cmd byte, subID byte, msgID []byte) (AckResponse, error) {
	if s.closed.Load() {
		return AckResponse{}, ErrStreamClosed
	}

	buf := pool.Get(10)
	defer pool.Put(buf)

	ch := make(chan AckResponse, 1)
	defer close(ch)

	correlationID := s.ac.Next(ch)
	defer s.ac.Delete(correlationID)

	buf = append(buf, cmd)
	buf = binary.BigEndian.AppendUint32(buf, correlationID)
	buf = append(buf, subID)
	buf = binary.BigEndian.AppendUint32(buf, 1)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msgID)))

	s.out.EnqueueProtoMulti(buf, msgID)

	select {
	case <-time.After(s.conn.timeout):
		return AckResponse{}, ErrTimeout
	case resp := <-ch:
		return resp, nil
	}
}

func (s *stream) sendTxCmd(cmd byte) error {
	if s.closed.Load() {
		return ErrStreamClosed
	}

	buf := pool.Get(5)
	defer pool.Put(buf)

	ch := make(chan error, 1)
	defer close(ch)

	id := s.cm.Next(ch)
	defer s.cm.Delete(id)

	buf = append(buf, cmd)
	buf = binary.BigEndian.AppendUint32(buf, id)

	s.out.EnqueueProto(buf)

	select {
	case <-time.After(s.conn.timeout):
		return ErrTimeout
	case err := <-ch:
		return err
	}
}

func (s *stream) readLoop() {
	defer s.wg.Done()

	buf := pool.Get(ReadBufferSize)[:ReadBufferSize]
	defer pool.Put(buf)

	for {
		n, err := s.quicStream.Read(buf)
		if err != nil {
			if err == io.EOF {
				if n != 0 {
					err = s.parse(buf[:n])
					if err != nil {
						s.l.Error("writer read loop", "err", err)
						return
					}
				}
				return
			}
		}

		if n == 0 {
			continue
		}

		err = s.parse(buf[:n])
		if err != nil {
			s.l.Error("writer read loop", "err", err)
			return
		}
	}
}

func (s *stream) parse(buf []byte) error {
	var (
		i int
		b byte
	)

	for i = 0; i < len(buf); i++ {
		b = buf[i]

		switch s.ps.state {
		case OP_START:
			switch b {
			case byte(v1proto.RESP_CODE_PRODUCE):
				s.ps.state = OP_PRODUCE
			case byte(v1proto.RESP_CODE_MSG):
				s.ps.state = OP_MSG
			case byte(v1proto.RESP_CODE_FETCH):
				s.ps.state = OP_FETCH
			case byte(v1proto.RESP_CODE_HPRODUCE):
				s.ps.state = OP_PRODUCE_H
			case byte(v1proto.RESP_CODE_HMSG):
				s.ps.state = OP_MSG_H
			case byte(v1proto.RESP_CODE_HFETCH):
				s.ps.state = OP_FETCH_H
			case byte(v1proto.RESP_CODE_ACK):
				s.ps.state = OP_ACK
			case byte(v1proto.RESP_CODE_NACK):
				s.ps.state = OP_NACK
			case byte(v1proto.RESP_CODE_TX_BEGIN):
				s.ps.state = OP_TX_BEGIN
			case byte(v1proto.RESP_CODE_TX_COMMIT):
				s.ps.state = OP_TX_COMMIT
			case byte(v1proto.RESP_CODE_TX_ROLLBACK):
				s.ps.state = OP_TX_ROLLBACK
			case byte(v1proto.OP_CODE_PING):
				s.writePong()
			case byte(v1proto.RESP_CODE_SUBSCRIBE):
				s.ps.state = OP_SUBSCRIBE
			case byte(v1proto.RESP_CODE_HSUBSCRIBE):
				s.ps.state = OP_SUBSCRIBE_H
			case byte(v1proto.RESP_CODE_UNSUBSCRIBE):
				s.ps.state = OP_UNSUBSCRIBE
			case byte(v1proto.RESP_CODE_DISCONNECT):
				close(s.disconnectCh)
				return nil
			case byte(v1proto.OP_CODE_STOP):
				go s.Close() // we probably can do something smarter here
				return nil
			}
		case OP_PRODUCE, OP_PRODUCE_H:
			s.ps.ca.cID = pool.Get(Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_CORRELATION_ID_ARG
		case OP_CORRELATION_ID_ARG:
			toCopy := Uint32Len - len(s.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(s.ps.ca.cID)
				s.ps.ca.cID = s.ps.ca.cID[:start+toCopy]
				copy(s.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				s.ps.ca.cID = append(s.ps.ca.cID, b)
			}

			if len(s.ps.ca.cID) >= Uint32Len {
				s.ps.ca.cIDUint32 = binary.BigEndian.Uint32(s.ps.ca.cID)
				s.ps.state = OP_ERROR_CODE_ARG
				pool.Put(s.ps.ca.cID)
			}
		case OP_ERROR_CODE_ARG:
			switch b {
			case byte(v1proto.ERR_CODE_NO):
				s.cm.Send(s.ps.ca.cIDUint32, nil)
				s.ps.ca, s.ps.state = correlationIDArg{}, OP_START
				continue
			case byte(v1proto.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(Uint32Len)
				s.ps.state = OP_ERROR_PAYLOAD_ARG
			default:
				s.quicStream.Close()
				return ErrParseProto
			}
		case OP_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					s.quicStream.Close()
					return fmt.Errorf("parse write err len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_ERROR_PAYLOAD
			}
		case OP_ERROR_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ea.errLen) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.cm.Send(s.ps.ca.cIDUint32, errors.New(string(s.ps.payloadBuf)))
					pool.Put(s.ps.payloadBuf)
					s.ps.ca.cID, s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, nil, correlationIDArg{}, errArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.cm.Send(s.ps.ca.cIDUint32, errors.New(string(s.ps.payloadBuf)))
					pool.Put(s.ps.payloadBuf)
					s.ps.ca.cID, s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, nil, correlationIDArg{}, errArg{}, OP_START
				}
			}
		case OP_MSG:
			sub, ok := s.subs.get(b)
			if !ok {
				return errors.New("subscription not found")
			}
			s.ps.ma.sub = sub
			if sub.autoCommit {
				s.ps.state = OP_MSG_ARG
				continue
			}
			s.ps.argBuf = pool.Get(Uint32Len)
			s.ps.state = OP_MSG_ID_ARG
		case OP_MSG_ID_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				s.ps.ma.idLen = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.metaBuf = nil, pool.Get(int(s.ps.ma.idLen))
				if s.ps.ma.idLen <= 0 {
					s.ps.state = OP_MSG_ARG
					continue
				}
				s.ps.state = OP_MSG_ID_PAYLOAD
			}
		case OP_MSG_ID_PAYLOAD:
			s.ps.metaBuf = append(s.ps.metaBuf, b)
			if len(s.ps.metaBuf) >= int(s.ps.ma.idLen) {
				s.ps.argBuf = pool.Get(Uint32Len)
				s.ps.state = OP_MSG_ARG
			}
		case OP_MSG_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				if err := s.parseMsgLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					s.quicStream.Close()
					return fmt.Errorf("parse msg len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_MSG_PAYLOAD
			}
		case OP_MSG_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ma.len) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ma.len) {
					s.ps.ma.sub.msgs <- fujinMsg{
						Value:   s.ps.payloadBuf,
						Headers: s.ps.ma.headers,
						id:      s.ps.metaBuf,
						subID:   s.ps.ma.sub.id,
						s:       s,
					}
					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.state = nil, nil, msgArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ma.len))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ma.len) {
					s.ps.ma.sub.msgs <- fujinMsg{
						Value:   s.ps.payloadBuf,
						Headers: s.ps.ma.headers,
						id:      s.ps.metaBuf,
						subID:   s.ps.ma.sub.id,
						s:       s,
					}
					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.state = nil, nil, msgArg{}, OP_START
				}
			}
		case OP_MSG_H:
			sub, ok := s.subs.get(b)
			if !ok {
				return errors.New("subscription not found")
			}
			s.ps.ma.sub = sub
			s.ps.argBuf = pool.Get(Uint16Len)
			s.ps.state = OP_MSG_H_HEADERS_COUNT_ARG
		case OP_MSG_H_HEADERS_COUNT_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint16Len {
				s.ps.ma.headerCount = binary.BigEndian.Uint16(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf = nil
				if s.ps.ma.headerCount <= 0 {
					if s.ps.ma.sub.autoCommit {
						s.ps.state = OP_MSG_ARG
						continue
					}
					s.ps.argBuf = pool.Get(Uint32Len)
					s.ps.state = OP_MSG_ID_ARG
				}
				s.ps.ma.headers = make(map[string]string, s.ps.ma.headerCount/2)
				s.ps.ma.headerStep = 0
				s.ps.state = OP_MSG_H_HEADER_LEN
			}
		case OP_MSG_H_HEADER_LEN:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				s.ps.ma.headerLen = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf = nil
				s.ps.ma.headerBuf = pool.Get(int(s.ps.ma.headerLen))
				s.ps.state = OP_MSG_H_HEADER_PAYLOAD
			}
		case OP_MSG_H_HEADER_PAYLOAD:
			s.ps.ma.headerBuf = append(s.ps.ma.headerBuf, b)
			if len(s.ps.ma.headerBuf) >= int(s.ps.ma.headerLen) {
				if s.ps.ma.headerStep%2 == 0 {
					s.ps.ma.headerKey = string(s.ps.ma.headerBuf)
				} else {
					s.ps.ma.headers[s.ps.ma.headerKey] = string(s.ps.ma.headerBuf)
				}
				pool.Put(s.ps.ma.headerBuf)
				s.ps.ma.headerBuf = nil
				s.ps.ma.headerLen = 0
				s.ps.ma.headerStep++
				if s.ps.ma.headerStep < int(s.ps.ma.headerCount) {
					s.ps.state = OP_MSG_H_HEADER_LEN
				} else {
					if s.ps.ma.sub.autoCommit {
						s.ps.state = OP_MSG_ARG
						continue
					}
					s.ps.argBuf = pool.Get(Uint32Len)
					s.ps.state = OP_MSG_ID_ARG
				}
			}
		case OP_TX_BEGIN:
			s.ps.ca.cID = pool.Get(Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_CORRELATION_ID_ARG
		case OP_TX_COMMIT:
			s.ps.ca.cID = pool.Get(Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_CORRELATION_ID_ARG
		case OP_TX_ROLLBACK:
			s.ps.ca.cID = pool.Get(Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_CORRELATION_ID_ARG
		case OP_FETCH:
			s.ps.ca.cID = pool.Get(Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_FETCH_CORRELATION_ID_ARG
		case OP_FETCH_CORRELATION_ID_ARG:
			toCopy := Uint32Len - len(s.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(s.ps.ca.cID)
				s.ps.ca.cID = s.ps.ca.cID[:start+toCopy]
				copy(s.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				s.ps.ca.cID = append(s.ps.ca.cID, b)
			}

			if len(s.ps.ca.cID) >= Uint32Len {
				s.ps.ca.cIDUint32 = binary.BigEndian.Uint32(s.ps.ca.cID)
				s.ps.fa.msgs, s.ps.fa.autoCommit, s.ps.fa.err = s.fcm.get(s.ps.ca.cIDUint32)
				s.ps.argBuf = pool.Get(Uint32Len)
				s.ps.state = OP_FETCH_ERROR_CODE_ARG
			}
		case OP_FETCH_ERROR_CODE_ARG:
			switch b {
			case byte(v1proto.ERR_CODE_NO):
				pool.Put(s.ps.ca.cID)
				s.ps.state = OP_FETCH_SUBSCRIPTION_ID_ARG
				continue
			case byte(v1proto.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(Uint32Len)
				s.ps.state = OP_FETCH_ERROR_PAYLOAD_ARG
			default:
				s.quicStream.Close()
				return ErrParseProto
			}
		case OP_FETCH_SUBSCRIPTION_ID_ARG:
			s.ps.fa.subID = b
			s.ps.argBuf = pool.Get(Uint32Len)
			s.ps.state = OP_FETCH_N_ARG
		case OP_FETCH_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					pool.Put(s.ps.ca.cID)
					s.quicStream.Close()
					err = fmt.Errorf("parse write err len arg: %w", err)
					s.ps.fa.err <- err
					close(s.ps.fa.err)
					return err
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_FETCH_ERROR_PAYLOAD
			}
		case OP_FETCH_ERROR_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ea.errLen) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.ps.fa.err <- errors.New(string(s.ps.payloadBuf))
					close(s.ps.fa.err)
					s.ps.ca, s.ps.fa, s.ps.payloadBuf, s.ps.ea, s.ps.state = correlationIDArg{}, fetchArg{}, nil, errArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = make([]byte, 0, s.ps.ea.errLen)
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.ps.fa.err <- errors.New(string(s.ps.payloadBuf))
					close(s.ps.fa.err)
					s.ps.ca, s.ps.fa, s.ps.payloadBuf, s.ps.ea, s.ps.state = correlationIDArg{}, fetchArg{}, nil, errArg{}, OP_START
				}
			}
		case OP_FETCH_N_ARG:
			toCopy := Uint32Len - len(s.ps.argBuf)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(s.ps.argBuf)
				s.ps.argBuf = s.ps.argBuf[:start+toCopy]
				copy(s.ps.argBuf[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				s.ps.argBuf = append(s.ps.argBuf, b)
			}

			if len(s.ps.argBuf) >= Uint32Len {
				s.ps.fa.n = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf = pool.Get(Uint32Len)
				if s.ps.fa.autoCommit {
					s.ps.state = OP_FETCH_MSG_ARG
					continue
				}
				s.ps.state = OP_FETCH_MSG_ID_ARG
			}
		case OP_FETCH_MSG_ID_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				s.ps.ma.idLen = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.metaBuf = nil, make([]byte, 0, s.ps.ma.idLen)
				if s.ps.ma.idLen <= 0 {
					s.ps.state = OP_FETCH_MSG_ARG
					continue
				}
				s.ps.state = OP_FETCH_MSG_ID_PAYLOAD
			}
		case OP_FETCH_MSG_ID_PAYLOAD:
			s.ps.metaBuf = append(s.ps.metaBuf, b)
			if len(s.ps.metaBuf) >= int(s.ps.ma.idLen) {
				s.ps.argBuf = pool.Get(Uint32Len)
				s.ps.state = OP_FETCH_MSG_ARG
			}
		case OP_FETCH_MSG_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				if err := s.parseMsgLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					_ = s.quicStream.Close()
					return fmt.Errorf("parse msg len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_FETCH_MSG_PAYLOAD
			}
		case OP_FETCH_MSG_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ma.len) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ma.len) {
					s.ps.fa.msgs = append(s.ps.fa.msgs, fujinMsg{
						Value: s.ps.payloadBuf,
						id:    s.ps.metaBuf,
						s:     s,
					})
					s.ps.fa.handled++
					if s.ps.fa.handled >= s.ps.fa.n {
						s.fcm.setMsgs(s.ps.ca.cIDUint32, s.ps.fa.msgs)
						close(s.ps.fa.err)
						s.ps.metaBuf, s.ps.payloadBuf, s.ps.ca, s.ps.ma, s.ps.fa, s.ps.state = nil, nil, correlationIDArg{}, msgArg{}, fetchArg{}, OP_START
						continue
					}

					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma = nil, nil, msgArg{}
					if s.ps.fa.autoCommit {
						s.ps.state = OP_FETCH_MSG_ARG
						continue
					}
					s.ps.state = OP_FETCH_MSG_ID_ARG
				}
			} else {
				s.ps.payloadBuf = make([]byte, 0, s.ps.ma.len)
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ma.len) {
					s.ps.fa.msgs = append(s.ps.fa.msgs, fujinMsg{
						Value: s.ps.payloadBuf,
						id:    s.ps.metaBuf,
						s:     s,
					})
					s.ps.fa.handled++
					if s.ps.fa.handled >= s.ps.fa.n {
						s.fcm.setMsgs(s.ps.ca.cIDUint32, s.ps.fa.msgs)
						close(s.ps.fa.err)
						s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.fa, s.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
					}
					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.fa, s.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
				}
			}
		case OP_FETCH_H:
			s.ps.ca.cID = pool.Get(Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_FETCH_H_CORRELATION_ID_ARG
		case OP_FETCH_H_CORRELATION_ID_ARG:
			toCopy := Uint32Len - len(s.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(s.ps.ca.cID)
				s.ps.ca.cID = s.ps.ca.cID[:start+toCopy]
				copy(s.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				s.ps.ca.cID = append(s.ps.ca.cID, b)
			}

			if len(s.ps.ca.cID) >= Uint32Len {
				s.ps.ca.cIDUint32 = binary.BigEndian.Uint32(s.ps.ca.cID)
				s.ps.fa.msgs, s.ps.fa.autoCommit, s.ps.fa.err = s.fcm.get(s.ps.ca.cIDUint32)
				s.ps.argBuf = pool.Get(Uint32Len)
				s.ps.state = OP_FETCH_H_ERROR_CODE_ARG
			}
		case OP_FETCH_H_ERROR_CODE_ARG:
			switch b {
			case byte(v1proto.ERR_CODE_NO):
				pool.Put(s.ps.ca.cID)
				s.ps.state = OP_FETCH_H_SUBSCRIPTION_ID_ARG
				continue
			case byte(v1proto.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(Uint32Len)
				s.ps.state = OP_FETCH_H_ERROR_PAYLOAD_ARG
			default:
				s.quicStream.Close()
				return ErrParseProto
			}
		case OP_FETCH_H_SUBSCRIPTION_ID_ARG:
			s.ps.fa.subID = b
			s.ps.argBuf = pool.Get(Uint32Len)
			s.ps.state = OP_FETCH_H_N_ARG
		case OP_FETCH_H_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					pool.Put(s.ps.ca.cID)
					s.quicStream.Close()
					err = fmt.Errorf("parse write err len arg: %w", err)
					s.ps.fa.err <- err
					close(s.ps.fa.err)
					return err
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_FETCH_H_ERROR_PAYLOAD
			}
		case OP_FETCH_H_ERROR_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ea.errLen) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.ps.fa.err <- errors.New(string(s.ps.payloadBuf))
					close(s.ps.fa.err)
					s.ps.ca, s.ps.fa, s.ps.payloadBuf, s.ps.ea, s.ps.state = correlationIDArg{}, fetchArg{}, nil, errArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = make([]byte, 0, s.ps.ea.errLen)
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.ps.fa.err <- errors.New(string(s.ps.payloadBuf))
					close(s.ps.fa.err)
					s.ps.ca, s.ps.fa, s.ps.payloadBuf, s.ps.ea, s.ps.state = correlationIDArg{}, fetchArg{}, nil, errArg{}, OP_START
				}
			}
		case OP_FETCH_H_N_ARG:
			toCopy := Uint32Len - len(s.ps.argBuf)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(s.ps.argBuf)
				s.ps.argBuf = s.ps.argBuf[:start+toCopy]
				copy(s.ps.argBuf[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				s.ps.argBuf = append(s.ps.argBuf, b)
			}

			if len(s.ps.argBuf) >= Uint32Len {
				s.ps.fa.n = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf = pool.Get(Uint32Len)
				s.ps.state = OP_FETCH_H_HEADERS_COUNT_ARG
			}
		case OP_FETCH_H_HEADERS_COUNT_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint16Len {
				s.ps.fa.headerCount = binary.BigEndian.Uint16(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf = nil
				if s.ps.ma.headerCount <= 0 {
					if s.ps.ma.sub.autoCommit {
						s.ps.state = OP_FETCH_H_MSG_ARG
						continue
					}
					s.ps.argBuf = pool.Get(Uint32Len)
					s.ps.state = OP_FETCH_H_MSG_ID_ARG
				}
				s.ps.fa.headers = make(map[string]string, s.ps.fa.headerCount/2)
				s.ps.fa.headerStep = 0
				s.ps.state = OP_FETCH_H_HEADER_LEN
			}
		case OP_FETCH_H_HEADER_LEN:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				s.ps.fa.headerLen = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf = nil
				s.ps.fa.headerBuf = pool.Get(int(s.ps.fa.headerLen))
				s.ps.state = OP_FETCH_H_HEADER_PAYLOAD
			}
		case OP_FETCH_H_HEADER_PAYLOAD:
			s.ps.fa.headerBuf = append(s.ps.fa.headerBuf, b)
			if len(s.ps.fa.headerBuf) >= int(s.ps.fa.headerLen) {
				if s.ps.fa.headerStep%2 == 0 {
					s.ps.fa.headerKey = string(s.ps.fa.headerBuf)
				} else {
					s.ps.fa.headers[s.ps.fa.headerKey] = string(s.ps.fa.headerBuf)
				}
				pool.Put(s.ps.fa.headerBuf)
				s.ps.fa.headerBuf = nil
				s.ps.fa.headerLen = 0
				s.ps.fa.headerStep++
				if s.ps.fa.headerStep < int(s.ps.fa.headerCount) {
					s.ps.state = OP_FETCH_H_HEADER_LEN
				} else {
					if s.ps.fa.autoCommit {
						s.ps.state = OP_FETCH_H_MSG_ARG
						continue
					}
					s.ps.argBuf = pool.Get(Uint32Len)
					s.ps.state = OP_FETCH_H_MSG_ID_ARG
				}
			}
		case OP_FETCH_H_MSG_ID_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				s.ps.ma.idLen = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.metaBuf = nil, make([]byte, 0, s.ps.ma.idLen)
				if s.ps.ma.idLen <= 0 {
					s.ps.state = OP_FETCH_H_MSG_ARG
					continue
				}
				s.ps.state = OP_FETCH_H_MSG_ID_PAYLOAD
			}
		case OP_FETCH_H_MSG_ID_PAYLOAD:
			s.ps.metaBuf = append(s.ps.metaBuf, b)
			if len(s.ps.metaBuf) >= int(s.ps.ma.idLen) {
				s.ps.argBuf = pool.Get(Uint32Len)
				s.ps.state = OP_FETCH_H_MSG_ARG
			}
		case OP_FETCH_H_MSG_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				if err := s.parseMsgLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					_ = s.quicStream.Close()
					return fmt.Errorf("parse msg len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_FETCH_H_MSG_PAYLOAD
			}
		case OP_FETCH_H_MSG_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ma.len) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ma.len) {
					s.ps.fa.msgs = append(s.ps.fa.msgs, fujinMsg{
						Value:   s.ps.payloadBuf,
						Headers: s.ps.fa.headers,
						id:      s.ps.metaBuf,
						s:       s,
					})
					s.ps.fa.handled++
					if s.ps.fa.handled >= s.ps.fa.n {
						s.fcm.setMsgs(s.ps.ca.cIDUint32, s.ps.fa.msgs)
						close(s.ps.fa.err)
						s.ps.metaBuf, s.ps.payloadBuf, s.ps.ca, s.ps.ma, s.ps.fa, s.ps.state = nil, nil, correlationIDArg{}, msgArg{}, fetchArg{}, OP_START
						continue
					}

					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma = nil, nil, msgArg{}
					if s.ps.fa.autoCommit {
						s.ps.state = OP_FETCH_H_MSG_ARG
						continue
					}
					s.ps.state = OP_FETCH_H_MSG_ID_ARG
				}
			} else {
				s.ps.payloadBuf = make([]byte, 0, s.ps.ma.len)
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ma.len) {
					s.ps.fa.msgs = append(s.ps.fa.msgs, fujinMsg{
						Value:   s.ps.payloadBuf,
						Headers: s.ps.fa.headers,
						id:      s.ps.metaBuf,
						s:       s,
					})
					s.ps.fa.handled++
					if s.ps.fa.handled >= s.ps.fa.n {
						s.fcm.setMsgs(s.ps.ca.cIDUint32, s.ps.fa.msgs)
						close(s.ps.fa.err)
						s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.fa, s.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
					}
					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.fa, s.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
				}
			}
		case OP_SUBSCRIBE, OP_SUBSCRIBE_H:
			s.ps.ca.cID = pool.Get(Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_SUBSCRIBE_CORRELATION_ID_ARG
		case OP_SUBSCRIBE_CORRELATION_ID_ARG:
			toCopy := Uint32Len - len(s.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(s.ps.ca.cID)
				s.ps.ca.cID = s.ps.ca.cID[:start+toCopy]
				copy(s.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				s.ps.ca.cID = append(s.ps.ca.cID, b)
			}

			if len(s.ps.ca.cID) >= Uint32Len {
				s.ps.ca.cIDUint32 = binary.BigEndian.Uint32(s.ps.ca.cID)
				pool.Put(s.ps.ca.cID)
				s.ps.state = OP_SUBSCRIBE_ERROR_CODE_ARG
			}
		case OP_SUBSCRIBE_ERROR_CODE_ARG:
			switch b {
			case byte(v1proto.ERR_CODE_NO):
				s.ps.state = OP_SUBSCRIBE_SUB_ID_ARG
				continue
			case byte(v1proto.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(Uint32Len)
				s.ps.state = OP_SUBSCRIBE_ERROR_PAYLOAD_ARG
			default:
				s.quicStream.Close()
				return ErrParseProto
			}
		case OP_SUBSCRIBE_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					s.quicStream.Close()
					return fmt.Errorf("parse subscribe err len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_SUBSCRIBE_ERROR_PAYLOAD
			}
		case OP_SUBSCRIBE_ERROR_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ea.errLen) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.sc.Send(s.ps.ca.cIDUint32, SubscribeResponse{Err: errors.New(string(s.ps.payloadBuf))})
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, correlationIDArg{}, errArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.sc.Send(s.ps.ca.cIDUint32, SubscribeResponse{Err: errors.New(string(s.ps.payloadBuf))})
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, correlationIDArg{}, errArg{}, OP_START
				}
			}
		case OP_SUBSCRIBE_SUB_ID_ARG:
			s.sc.Send(s.ps.ca.cIDUint32, SubscribeResponse{SubID: b})
			s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, correlationIDArg{}, errArg{}, OP_START
		case OP_UNSUBSCRIBE:
			s.ps.ca.cID = pool.Get(Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_UNSUBSCRIBE_CORRELATION_ID_ARG
		case OP_UNSUBSCRIBE_CORRELATION_ID_ARG:
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			if len(s.ps.ca.cID) >= Uint32Len {
				s.ps.ca.cIDUint32 = binary.BigEndian.Uint32(s.ps.ca.cID)
				pool.Put(s.ps.ca.cID)
				s.ps.state = OP_UNSUBSCRIBE_ERROR_CODE_ARG
			}
		case OP_UNSUBSCRIBE_ERROR_CODE_ARG:
			switch b {
			case byte(v1proto.ERR_CODE_NO):
				s.cm.Send(s.ps.ca.cIDUint32, nil)
				s.ps.ca, s.ps.state = correlationIDArg{}, OP_START
				continue
			case byte(v1proto.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(Uint32Len)
				s.ps.state = OP_UNSUBSCRIBE_ERROR_PAYLOAD_ARG
			default:
				s.quicStream.Close()
				return ErrParseProto
			}
		case OP_UNSUBSCRIBE_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					s.quicStream.Close()
					return fmt.Errorf("parse unsubscribe err len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_UNSUBSCRIBE_ERROR_PAYLOAD
			}
		case OP_UNSUBSCRIBE_ERROR_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ea.errLen) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.cm.Send(s.ps.ca.cIDUint32, errors.New(string(s.ps.payloadBuf)))
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, correlationIDArg{}, errArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.cm.Send(s.ps.ca.cIDUint32, errors.New(string(s.ps.payloadBuf)))
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, correlationIDArg{}, errArg{}, OP_START
				}
			}
		case OP_ACK, OP_NACK:
			s.ps.ca.cID = pool.Get(Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			switch s.ps.state {
			case OP_ACK:
				s.ps.state = OP_ACK_CORRELATION_ID_ARG
			case OP_NACK:
				s.ps.state = OP_NACK_CORRELATION_ID_ARG
			}
		case OP_ACK_CORRELATION_ID_ARG, OP_NACK_CORRELATION_ID_ARG:
			toCopy := Uint32Len - len(s.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(s.ps.ca.cID)
				s.ps.ca.cID = s.ps.ca.cID[:start+toCopy]
				copy(s.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				s.ps.ca.cID = append(s.ps.ca.cID, b)
			}

			if len(s.ps.ca.cID) >= Uint32Len {
				s.ps.ca.cIDUint32 = binary.BigEndian.Uint32(s.ps.ca.cID)
				pool.Put(s.ps.ca.cID)
				if s.ps.state == OP_ACK_CORRELATION_ID_ARG {
					s.ps.state = OP_ACK_ERROR_CODE_ARG
				} else {
					s.ps.state = OP_NACK_ERROR_CODE_ARG
				}
			}
		case OP_ACK_ERROR_CODE_ARG, OP_NACK_ERROR_CODE_ARG:
			switch b {
			case byte(v1proto.ERR_CODE_NO):
				s.ps.argBuf = pool.Get(Uint32Len)
				if s.ps.state == OP_ACK_ERROR_CODE_ARG {
					s.ps.state = OP_ACK_MSG_BATCH_LEN_ARG
				} else {
					s.ps.state = OP_NACK_MSG_BATCH_LEN_ARG
				}
			case byte(v1proto.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(Uint32Len)
				if s.ps.state == OP_ACK_ERROR_CODE_ARG {
					s.ps.state = OP_ACK_ERROR_PAYLOAD_ARG
				} else {
					s.ps.state = OP_NACK_ERROR_PAYLOAD_ARG
				}
			default:
				s.quicStream.Close()
				return ErrParseProto
			}
		case OP_ACK_ERROR_PAYLOAD_ARG, OP_NACK_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					s.quicStream.Close()
					return fmt.Errorf("parse ack/nack err len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf = nil
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				if s.ps.state == OP_ACK_ERROR_PAYLOAD_ARG {
					s.ps.state = OP_ACK_ERROR_PAYLOAD
				} else {
					s.ps.state = OP_NACK_ERROR_PAYLOAD
				}
			}
		case OP_ACK_ERROR_PAYLOAD, OP_NACK_ERROR_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ea.errLen) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					err := errors.New(string(s.ps.payloadBuf))
					s.ac.Send(s.ps.ca.cIDUint32, AckResponse{Err: err})
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.aa, s.ps.state = nil, correlationIDArg{}, errArg{}, ackArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					err := errors.New(string(s.ps.payloadBuf))
					s.ac.Send(s.ps.ca.cIDUint32, AckResponse{Err: err})
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.aa, s.ps.state = nil, correlationIDArg{}, errArg{}, ackArg{}, OP_START
				}
			}
		case OP_ACK_MSG_BATCH_LEN_ARG, OP_NACK_MSG_BATCH_LEN_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				s.ps.aa.n = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf = nil
				s.ps.aa.resps = make([]AckMsgResponse, 0, s.ps.aa.n)
				if s.ps.aa.n == 0 {
					s.ac.Send(s.ps.ca.cIDUint32, AckResponse{AckMsgResponses: s.ps.aa.resps})
					s.ps.ca, s.ps.aa, s.ps.state = correlationIDArg{}, ackArg{}, OP_START
					continue
				}
				s.ps.argBuf = pool.Get(Uint32Len)
				if s.ps.state == OP_ACK_MSG_BATCH_LEN_ARG {
					s.ps.state = OP_ACK_MSG_ID_ARG
				} else {
					s.ps.state = OP_NACK_MSG_ID_ARG
				}
			}
		case OP_ACK_MSG_ID_ARG, OP_NACK_MSG_ID_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				s.ps.aa.currMsgIDLen = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf = nil
				if s.ps.aa.currMsgIDLen == 0 {
					if s.ps.state == OP_ACK_MSG_ID_ARG {
						s.ps.state = OP_ACK_MSG_ERROR_CODE_ARG
					} else {
						s.ps.state = OP_NACK_MSG_ERROR_CODE_ARG
					}
					continue
				}
				s.ps.metaBuf = make([]byte, 0, s.ps.aa.currMsgIDLen)
				if s.ps.state == OP_ACK_MSG_ID_ARG {
					s.ps.state = OP_ACK_MSG_ID_PAYLOAD
				} else {
					s.ps.state = OP_NACK_MSG_ID_PAYLOAD
				}
			}
		case OP_ACK_MSG_ID_PAYLOAD, OP_NACK_MSG_ID_PAYLOAD:
			s.ps.metaBuf = append(s.ps.metaBuf, b)
			if len(s.ps.metaBuf) >= int(s.ps.aa.currMsgIDLen) {
				msgID := make([]byte, len(s.ps.metaBuf))
				copy(msgID, s.ps.metaBuf)
				s.ps.aa.resps = append(s.ps.aa.resps, AckMsgResponse{MsgID: msgID})
				s.ps.metaBuf = nil
				if s.ps.state == OP_ACK_MSG_ID_PAYLOAD {
					s.ps.state = OP_ACK_MSG_ERROR_CODE_ARG
				} else {
					s.ps.state = OP_NACK_MSG_ERROR_CODE_ARG
				}
			}
		case OP_ACK_MSG_ERROR_CODE_ARG, OP_NACK_MSG_ERROR_CODE_ARG:
			switch b {
			case byte(v1proto.ERR_CODE_NO):
				// No error for this message, move to next message or finish
				if uint32(len(s.ps.aa.resps)) >= s.ps.aa.n {
					// All messages processed
					s.ac.Send(s.ps.ca.cIDUint32, AckResponse{AckMsgResponses: s.ps.aa.resps})
					s.ps.ca, s.ps.aa, s.ps.state = correlationIDArg{}, ackArg{}, OP_START
					continue
				}
				// Process next message
				s.ps.argBuf = pool.Get(Uint32Len)
				if s.ps.state == OP_ACK_MSG_ERROR_CODE_ARG {
					s.ps.state = OP_ACK_MSG_ID_ARG
				} else {
					s.ps.state = OP_NACK_MSG_ID_ARG
				}
			case byte(v1proto.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(Uint32Len)
				if s.ps.state == OP_ACK_MSG_ERROR_CODE_ARG {
					s.ps.state = OP_ACK_MSG_ERROR_PAYLOAD_ARG
				} else {
					s.ps.state = OP_NACK_MSG_ERROR_PAYLOAD_ARG
				}
			default:
				s.quicStream.Close()
				return ErrParseProto
			}
		case OP_ACK_MSG_ERROR_PAYLOAD_ARG, OP_NACK_MSG_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					s.quicStream.Close()
					return fmt.Errorf("parse ack/nack msg err len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf = nil
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				if s.ps.state == OP_ACK_MSG_ERROR_PAYLOAD_ARG {
					s.ps.state = OP_ACK_MSG_ERROR_PAYLOAD
				} else {
					s.ps.state = OP_NACK_MSG_ERROR_PAYLOAD
				}
			}
		case OP_ACK_MSG_ERROR_PAYLOAD, OP_NACK_MSG_ERROR_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ea.errLen) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					// Update the last response with error
					if len(s.ps.aa.resps) > 0 {
						lastIdx := len(s.ps.aa.resps) - 1
						s.ps.aa.resps[lastIdx].Err = errors.New(string(s.ps.payloadBuf))
					}
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf = nil
					s.ps.ea = errArg{}

					// Check if all messages processed
					if uint32(len(s.ps.aa.resps)) >= s.ps.aa.n {
						s.ac.Send(s.ps.ca.cIDUint32, AckResponse{AckMsgResponses: s.ps.aa.resps})
						s.ps.ca, s.ps.aa, s.ps.state = correlationIDArg{}, ackArg{}, OP_START
						continue
					}
					// Process next message
					s.ps.argBuf = pool.Get(Uint32Len)
					if s.ps.state == OP_ACK_MSG_ERROR_PAYLOAD {
						s.ps.state = OP_ACK_MSG_ID_ARG
					} else {
						s.ps.state = OP_NACK_MSG_ID_ARG
					}
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					// Update the last response with error
					if len(s.ps.aa.resps) > 0 {
						lastIdx := len(s.ps.aa.resps) - 1
						s.ps.aa.resps[lastIdx].Err = errors.New(string(s.ps.payloadBuf))
					}
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf = nil
					s.ps.ea = errArg{}

					// Check if all messages processed
					if uint32(len(s.ps.aa.resps)) >= s.ps.aa.n {
						s.ac.Send(s.ps.ca.cIDUint32, AckResponse{AckMsgResponses: s.ps.aa.resps})
						s.ps.ca, s.ps.aa, s.ps.state = correlationIDArg{}, ackArg{}, OP_START
						continue
					}
					// Process next message
					s.ps.argBuf = pool.Get(Uint32Len)
					if s.ps.state == OP_ACK_MSG_ERROR_PAYLOAD {
						s.ps.state = OP_ACK_MSG_ID_ARG
					} else {
						s.ps.state = OP_NACK_MSG_ID_ARG
					}
				}
			}
		default:
			s.quicStream.Close()
			return ErrParseProto
		}
	}

	return nil
}

func (s *stream) writePong() {
	s.out.EnqueueProto(PONG_RESP)
}

func (s *stream) CheckParseStateAfterOpForTests() error {
	if s.ps.state != OP_START {
		return fmt.Errorf("invalid state: %d", s.ps.state)
	}

	if s.ps.argBuf != nil {
		return errors.New("arg buf is not nil")
	}
	if s.ps.metaBuf != nil {
		return errors.New("meta buf is not nil")
	}
	if s.ps.payloadBuf != nil {
		return errors.New("payload buf is not nil")
	}

	ea := errArg{}
	if s.ps.ea != ea {
		return errors.New("err arg is not empty")
	}

	// ma := msgArg{}
	// if s.ps.ma != ma {
	// 	return errors.New("msg arg is not empty")
	// }

	if s.ps.fa.n != 0 || s.ps.fa.err != nil || s.ps.fa.handled != 0 || len(s.ps.fa.msgs) != 0 {
		return errors.New("fetch arg is not empty")
	}

	if s.ps.aa.currMsgIDLen != 0 || s.ps.aa.n != 0 || len(s.ps.aa.resps) != 0 {
		return errors.New("ack arg is not empty")
	}

	if s.ps.ca.cID != nil || s.ps.ca.cIDUint32 != 0 {
		return errors.New("correlation id arg is not empty")
	}

	return nil
}
