package v1

import "encoding/binary"

const (
	Uint16Len = 2
	Uint32Len = 4
)

func AppendFujinUint16StringArray(buf []byte, m map[string]string) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(m)))
	for k, v := range m {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

