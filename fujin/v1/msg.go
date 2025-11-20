package v1

import (
	"errors"

	"github.com/fujin-io/fujin-go/models"
)

// fujinMsg is the internal message type used within the fujin protocol implementation
type fujinMsg struct {
	Value   []byte
	Headers map[string]string
	id      []byte
	subID   byte
	s       *stream
}

// toModelsMsg converts fujinMsg to models.Msg
func (m *fujinMsg) toModelsMsg() models.Msg {
	return models.Msg{
		SubscriptionID: uint32(m.subID),
		MessageID:      m.id,
		Payload:        m.Value,
		Headers:        m.Headers,
	}
}

// fromModelsMsg creates a fujinMsg from models.Msg (for internal use)
func fromModelsMsg(msg models.Msg) fujinMsg {
	return fujinMsg{
		Value:   msg.Payload,
		Headers: msg.Headers,
		id:      msg.MessageID,
		subID:   byte(msg.SubscriptionID),
	}
}

func (m *fujinMsg) Ack() error {
	if len(m.id) <= 0 {
		return nil
	}

	resp, err := m.s.ack(m.subID, m.id)
	if err != nil {
		return err
	}

	if len(resp.AckMsgResponses) < 1 {
		return errors.New("invalid response")
	}

	return resp.AckMsgResponses[0].Err
}

func (m *fujinMsg) Nack() error {
	if len(m.id) <= 0 {
		return nil
	}

	resp, err := m.s.nack(m.subID, m.id)
	if err != nil {
		return err
	}

	if len(resp.AckMsgResponses) < 1 {
		return errors.New("invalid response")
	}

	return resp.AckMsgResponses[0].Err
}

func (m fujinMsg) String() string {
	return string(m.Value)
}

