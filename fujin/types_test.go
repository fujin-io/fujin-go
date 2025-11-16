package fujin_test

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/fujin-io/fujin-go/fujin"
	"github.com/stretchr/testify/assert"
)

func TestConstants(t *testing.T) {
	assert.Equal(t, 2, fujin.Uint16Len)
	assert.Equal(t, 4, fujin.Uint32Len)
}

func TestAppendFujinUint16StringArray(t *testing.T) {
	t.Run("empty map", func(t *testing.T) {
		buf := make([]byte, 0)
		m := make(map[string]string)

		result := fujin.AppendFujinUint16StringArray(buf, m)

		// Should have 2 bytes for the map length (uint16)
		assert.Len(t, result, 2)

		// Map length should be 0
		mapLen := binary.BigEndian.Uint16(result[0:2])
		assert.Equal(t, uint16(0), mapLen)
	})

	t.Run("single entry", func(t *testing.T) {
		buf := make([]byte, 0)
		m := map[string]string{
			"key1": "value1",
		}

		result := fujin.AppendFujinUint16StringArray(buf, m)

		// Expected length: 2 (map len) + 4 (key len) + 4 (key) + 4 (value len) + 6 (value)
		expectedLen := 2 + 4 + 4 + 4 + 6
		assert.Len(t, result, expectedLen)

		// Verify map length
		mapLen := binary.BigEndian.Uint16(result[0:2])
		assert.Equal(t, uint16(1), mapLen)

		// Verify key length
		keyLen := binary.BigEndian.Uint32(result[2:6])
		assert.Equal(t, uint32(4), keyLen)

		// Verify key
		key := string(result[6:10])
		assert.Equal(t, "key1", key)

		// Verify value length
		valueLen := binary.BigEndian.Uint32(result[10:14])
		assert.Equal(t, uint32(6), valueLen)

		// Verify value
		value := string(result[14:20])
		assert.Equal(t, "value1", value)
	})

	t.Run("multiple entries", func(t *testing.T) {
		buf := make([]byte, 0)
		m := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		result := fujin.AppendFujinUint16StringArray(buf, m)

		// Verify map length
		mapLen := binary.BigEndian.Uint16(result[0:2])
		assert.Equal(t, uint16(3), mapLen)

		// Since maps are unordered in Go, we need to verify all entries exist
		// but we can't rely on order. Let's parse all entries.
		offset := 2
		entries := make(map[string]string)

		for i := 0; i < 3; i++ {
			// Read key length
			keyLen := binary.BigEndian.Uint32(result[offset : offset+4])
			offset += 4

			// Read key
			key := string(result[offset : offset+int(keyLen)])
			offset += int(keyLen)

			// Read value length
			valueLen := binary.BigEndian.Uint32(result[offset : offset+4])
			offset += 4

			// Read value
			value := string(result[offset : offset+int(valueLen)])
			offset += int(valueLen)

			entries[key] = value
		}

		// Verify all entries are present
		assert.Equal(t, "value1", entries["key1"])
		assert.Equal(t, "value2", entries["key2"])
		assert.Equal(t, "value3", entries["key3"])
	})

	t.Run("empty key", func(t *testing.T) {
		buf := make([]byte, 0)
		m := map[string]string{
			"": "value1",
		}

		result := fujin.AppendFujinUint16StringArray(buf, m)

		// Verify map length
		mapLen := binary.BigEndian.Uint16(result[0:2])
		assert.Equal(t, uint16(1), mapLen)

		// Verify key length is 0
		keyLen := binary.BigEndian.Uint32(result[2:6])
		assert.Equal(t, uint32(0), keyLen)

		// Verify value length
		valueLen := binary.BigEndian.Uint32(result[6:10])
		assert.Equal(t, uint32(6), valueLen)

		// Verify value
		value := string(result[10:16])
		assert.Equal(t, "value1", value)
	})

	t.Run("empty value", func(t *testing.T) {
		buf := make([]byte, 0)
		m := map[string]string{
			"key1": "",
		}

		result := fujin.AppendFujinUint16StringArray(buf, m)

		// Verify map length
		mapLen := binary.BigEndian.Uint16(result[0:2])
		assert.Equal(t, uint16(1), mapLen)

		// Verify key length
		keyLen := binary.BigEndian.Uint32(result[2:6])
		assert.Equal(t, uint32(4), keyLen)

		// Verify key
		key := string(result[6:10])
		assert.Equal(t, "key1", key)

		// Verify value length is 0
		valueLen := binary.BigEndian.Uint32(result[10:14])
		assert.Equal(t, uint32(0), valueLen)
	})

	t.Run("empty key and value", func(t *testing.T) {
		buf := make([]byte, 0)
		m := map[string]string{
			"": "",
		}

		result := fujin.AppendFujinUint16StringArray(buf, m)

		// Expected length: 2 (map len) + 4 (key len) + 0 (key) + 4 (value len) + 0 (value)
		expectedLen := 2 + 4 + 0 + 4 + 0
		assert.Len(t, result, expectedLen)

		// Verify map length
		mapLen := binary.BigEndian.Uint16(result[0:2])
		assert.Equal(t, uint16(1), mapLen)

		// Verify key length is 0
		keyLen := binary.BigEndian.Uint32(result[2:6])
		assert.Equal(t, uint32(0), keyLen)

		// Verify value length is 0
		valueLen := binary.BigEndian.Uint32(result[6:10])
		assert.Equal(t, uint32(0), valueLen)
	})

	t.Run("long key and value", func(t *testing.T) {
		buf := make([]byte, 0)
		longKey := strings.Repeat("a", 1000)
		longValue := strings.Repeat("b", 2000)
		m := map[string]string{
			longKey: longValue,
		}

		result := fujin.AppendFujinUint16StringArray(buf, m)

		// Expected length: 2 (map len) + 4 (key len) + 1000 (key) + 4 (value len) + 2000 (value)
		expectedLen := 2 + 4 + 1000 + 4 + 2000
		assert.Len(t, result, expectedLen)

		// Verify map length
		mapLen := binary.BigEndian.Uint16(result[0:2])
		assert.Equal(t, uint16(1), mapLen)

		// Verify key length
		keyLen := binary.BigEndian.Uint32(result[2:6])
		assert.Equal(t, uint32(1000), keyLen)

		// Verify value length (starts after key: 2 + 4 + 1000 = 1006)
		valueLen := binary.BigEndian.Uint32(result[1006:1010])
		assert.Equal(t, uint32(2000), valueLen)
	})

	t.Run("special characters in key and value", func(t *testing.T) {
		buf := make([]byte, 0)
		m := map[string]string{
			"key\n\t\r": "value\n\t\r",
		}

		result := fujin.AppendFujinUint16StringArray(buf, m)

		// Verify map length
		mapLen := binary.BigEndian.Uint16(result[0:2])
		assert.Equal(t, uint16(1), mapLen)

		// Verify key length
		keyLen := binary.BigEndian.Uint32(result[2:6])
		assert.Equal(t, uint32(6), keyLen) // "key\n\t\r" is 6 bytes

		// Verify key
		key := string(result[6:12])
		assert.Equal(t, "key\n\t\r", key)

		// Verify value length (starts after key: 2 + 4 + 6 = 12)
		valueLen := binary.BigEndian.Uint32(result[12:16])
		assert.Equal(t, uint32(8), valueLen) // "value\n\t\r" is 8 bytes

		// Verify value
		value := string(result[16:24])
		assert.Equal(t, "value\n\t\r", value)
	})

	t.Run("unicode characters", func(t *testing.T) {
		buf := make([]byte, 0)
		m := map[string]string{
			"key": "value",
		}

		result := fujin.AppendFujinUint16StringArray(buf, m)

		// Verify map length
		mapLen := binary.BigEndian.Uint16(result[0:2])
		assert.Equal(t, uint16(1), mapLen)

		// Verify key length (Cyrillic characters are multi-byte in UTF-8)
		keyLen := binary.BigEndian.Uint32(result[2:6])
		assert.Equal(t, uint32(8), keyLen) // "key" is 8 bytes in UTF-8

		// Verify key
		key := string(result[6:14])
		assert.Equal(t, "key", key)

		// Verify value length
		valueLen := binary.BigEndian.Uint32(result[14:18])
		assert.Equal(t, uint32(16), valueLen) // "value" is 16 bytes in UTF-8

		// Verify value
		value := string(result[18:34])
		assert.Equal(t, "value", value)
	})

	t.Run("appends to existing buffer", func(t *testing.T) {
		buf := []byte{0x01, 0x02, 0x03}
		m := map[string]string{
			"key1": "value1",
		}

		result := fujin.AppendFujinUint16StringArray(buf, m)

		// Should preserve original buffer content
		assert.Equal(t, byte(0x01), result[0])
		assert.Equal(t, byte(0x02), result[1])
		assert.Equal(t, byte(0x03), result[2])

		// Verify map length starts after original buffer
		mapLen := binary.BigEndian.Uint16(result[3:5])
		assert.Equal(t, uint16(1), mapLen)
	})
}
