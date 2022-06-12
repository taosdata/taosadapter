package jsontype

type JsonUint8 []uint8

var digits []uint32

func init() {
	digits = make([]uint32, 1000)
	for i := uint32(0); i < 1000; i++ {
		digits[i] = (((i / 100) + '0') << 16) + ((((i / 10) % 10) + '0') << 8) + i%10 + '0'
		if i < 10 {
			digits[i] += 2 << 24
		} else if i < 100 {
			digits[i] += 1 << 24
		}
	}
}
func writeFirstBuf(space []byte, v uint32) []byte {
	start := v >> 24
	if start == 0 {
		space = append(space, byte(v>>16), byte(v>>8))
	} else if start == 1 {
		space = append(space, byte(v>>8))
	}
	space = append(space, byte(v))
	return space
}

func (m JsonUint8) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	if len(m) == 0 {
		return []byte{'[', ']'}, nil
	}
	var b []byte
	b = append(b, '[')
	b = writeFirstBuf(b, digits[m[0]])
	for i := 1; i < len(m); i++ {
		b = append(b, ',')
		b = writeFirstBuf(b, digits[m[i]])
	}
	b = append(b, ']')
	return b, nil
}
