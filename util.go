package storage

import (
	"encoding/binary"
	"time"
)

type Record struct {
	Data []byte
	Time time.Time
}

func makeKey(prefix []byte, key []byte, timestamp time.Time) []byte {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(timestamp.UnixNano()))
	return append(append(prefix, key...), v...)
}

func extractTime(key []byte) time.Time {
	v := int64(binary.BigEndian.Uint64(key[len(key)-8:]))
	return time.Unix(v/int64(time.Second), v%int64(time.Second))
}