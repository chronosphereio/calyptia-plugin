package plugin

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ugorji/go/codec"
)

var _ codec.BytesExt = (*fTime)(nil)

// fTime implements codec.BytesExt to handle custom (de)serialization of types to/from []byte.
// It is used by codecs (e.g. binc, msgpack, simple) which do custom serialization of the types.
type fTime time.Time

func (*fTime) WriteExt(v interface{}) []byte {
	ft, ok := v.(*fTime)
	if !ok {
		panic(fmt.Sprintf("unexpected fluent time type %T", v))
	}

	t := time.Time(*ft).UTC()
	sec := t.Unix()
	nsec := t.UnixNano()

	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b, uint32(sec))
	binary.BigEndian.PutUint32(b[4:], uint32(nsec))

	return b
}

func (*fTime) ReadExt(dst interface{}, src []byte) {
	ft, ok := dst.(*fTime)
	if !ok {
		panic(fmt.Sprintf("unexpected fluent time type %T", dst))
	}

	if len(src) != 8 {
		panic(fmt.Sprintf("unexpected fluent time length %d", len(src)))
	}

	sec := binary.BigEndian.Uint32(src)
	nsec := binary.BigEndian.Uint32(src[4:])

	t := time.Unix(int64(sec), int64(nsec)).UTC()
	*ft = fTime(t)
}
