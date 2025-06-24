package plugin

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

const eventTimeBytesLen = 8

type EventTime struct {
	time.Time
}

var (
	_ msgpack.Marshaler   = (*EventTime)(nil)
	_ msgpack.Unmarshaler = (*EventTime)(nil)
)

func init() {
	msgpack.RegisterExt(0, (*EventTime)(nil))
}

func (tm *EventTime) MarshalMsgpack() ([]byte, error) {
	b := make([]byte, eventTimeBytesLen)
	// #nosec G115 event time uses 32 bit unix timestamps on fluent-bit side.
	binary.BigEndian.PutUint32(b, uint32(tm.Unix()))
	// #nosec G115 event time uses 32 bit unix timestamps on fluent-bit side.
	binary.BigEndian.PutUint32(b[4:], uint32(tm.Nanosecond()))
	return b, nil
}

func (tm *EventTime) UnmarshalMsgpack(b []byte) error {
	if len(b) != eventTimeBytesLen {
		return fmt.Errorf("invalid data length: got %d, wanted %d", len(b), eventTimeBytesLen)
	}
	sec := binary.BigEndian.Uint32(b)
	usec := binary.BigEndian.Uint32(b[4:])
	tm.Time = time.Unix(int64(sec), int64(usec))
	return nil
}
