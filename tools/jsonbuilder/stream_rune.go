package jsonbuilder

// Numbers fundamental to the encoding.
const (
	RuneError = '\uFFFD'     // the "error" Rune or "Unicode replacement character"
	RuneSelf  = 0x80         // characters below RuneSelf are represented as themselves in a single byte.
	MaxRune   = '\U0010FFFF' // Maximum valid Unicode code point.
	UTFMax    = 4            // maximum number of bytes of a UTF-8 encoded Unicode character.
)

// Code points in the surrogate range are not valid for UTF-8.
const (
	surrogateMin = 0xD800
	surrogateMax = 0xDFFF
)

const (
	t1 = 0b00000000
	tx = 0b10000000
	t2 = 0b11000000
	t3 = 0b11100000
	t4 = 0b11110000
	t5 = 0b11111000

	maskx = 0b00111111
	mask2 = 0b00011111
	mask3 = 0b00001111
	mask4 = 0b00000111

	rune1Max = 1<<7 - 1
	rune2Max = 1<<11 - 1
	rune3Max = 1<<16 - 1

	// The default lowest and highest continuation byte.
	locb = 0b10000000
	hicb = 0b10111111

	// These names of these constants are chosen to give nice alignment in the
	// table below. The first nibble is an index into acceptRanges or F for
	// special one-byte cases. The second nibble is the Rune length or the
	// Status for the special one-byte case.
	xx = 0xF1 // invalid: size 1
	as = 0xF0 // ASCII: size 1
	s1 = 0x02 // accept 0, size 2
	s2 = 0x13 // accept 1, size 3
	s3 = 0x03 // accept 0, size 3
	s4 = 0x23 // accept 2, size 3
	s5 = 0x34 // accept 3, size 4
	s6 = 0x04 // accept 0, size 4
	s7 = 0x44 // accept 4, size 4
)

func (stream *Stream) WriteRune(r rune) {
	if uint32(r) <= rune1Max {
		stream.WriteByte(byte(r))
		return
	}
	switch i := uint32(r); {
	case i <= rune2Max:
		stream.WriteByte(t2 | byte(r>>6))
		stream.WriteByte(tx | byte(r)&maskx)
		return
	case i > MaxRune, surrogateMin <= i && i <= surrogateMax:
		r = RuneError
		fallthrough
	case i <= rune3Max:
		stream.WriteByte(t3 | byte(r>>12))
		stream.WriteByte(tx | byte(r>>6)&maskx)
		stream.WriteByte(tx | byte(r)&maskx)
		return
	default:
		stream.WriteByte(t4 | byte(r>>18))
		stream.WriteByte(tx | byte(r>>12)&maskx)
		stream.WriteByte(tx | byte(r>>6)&maskx)
		stream.WriteByte(tx | byte(r)&maskx)
		return
	}
}

func (stream *Stream) WriteRuneString(r rune) {
	if uint32(r) <= rune1Max {
		stream.WriteStringByte(byte(r))
		return
	}
	switch i := uint32(r); {
	case i <= rune2Max:
		stream.WriteStringByte(t2 | byte(r>>6))
		stream.WriteStringByte(tx | byte(r)&maskx)
		return
	case i > MaxRune, surrogateMin <= i && i <= surrogateMax:
		r = RuneError
		fallthrough
	case i <= rune3Max:
		stream.WriteStringByte(t3 | byte(r>>12))
		stream.WriteStringByte(tx | byte(r>>6)&maskx)
		stream.WriteStringByte(tx | byte(r)&maskx)
		return
	default:
		stream.WriteStringByte(t4 | byte(r>>18))
		stream.WriteStringByte(tx | byte(r>>12)&maskx)
		stream.WriteStringByte(tx | byte(r>>6)&maskx)
		stream.WriteStringByte(tx | byte(r)&maskx)
		return
	}
}
