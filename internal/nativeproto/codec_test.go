package nativeproto

import (
	"encoding/binary"
	"testing"

	client "github.com/fujin-io/fujin-go"
	"github.com/stretchr/testify/require"
)

func TestProducePrefixMatchesCompleteFrame(t *testing.T) {
	headers := []client.Header{
		{Key: []byte("content-type"), Value: []byte("application/octet-stream")},
		{Key: []byte("tenant"), Value: []byte("test")},
	}
	payload := []byte("payload")
	prefix := ProducePrefix(OpHProduce, 42, "pub", len(payload), headers)
	frame := Produce(OpHProduce, 42, "pub", payload, headers)

	require.Equal(t, append(append([]byte(nil), prefix...), payload...), frame)
	require.Equal(t, uint32(len(payload)), binary.BigEndian.Uint32(prefix[len(prefix)-4:]))
	require.Equal(t, len(prefix), cap(prefix))
}

func TestProducePrefixOmitsRouteForTransactionalProduce(t *testing.T) {
	payload := []byte("transactional")
	prefix := ProducePrefix(OpTxProduce, 7, "ignored", len(payload), nil)
	frame := Produce(OpTxProduce, 7, "ignored", payload, nil)

	require.Equal(t, append(append([]byte(nil), prefix...), payload...), frame)
	require.Equal(t, 9, len(prefix))
}
