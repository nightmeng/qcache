package storage

import (
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	c, err := NewCache("cache")
	if err != nil {
		t.Fatalf("create cache failed, %s\n", err)
	}
	defer c.Close()

	ts := time.Now()

	record := &Record{
		Data: []byte("hello"),
		Time: ts,
	}

	if err := c.Put([]byte("xxxx"), record); err != nil {
		t.Fatalf("put failed, %s\n", err)
	}

	if record, err := c.Get([]byte("xxxx")); err != nil {
		t.Fatalf("get failed, %s\n", err)
	} else {
		if record == nil {
			t.Fatalf("get failed, no data\n")
		}
		if string(record.Data) != "hello" {
			t.Fatalf("get failed, data: %s\n", record.Data)
		}

		if record.Time.UnixNano() != ts.UnixNano() {
			t.Fatalf("get failed, invalid timestamp: %v, expect: %v\n", record.Time, ts)
		}
	}

	if err := c.Del([]byte("xxxx"), record); err != nil {
		t.Fatalf("del failed, %s\n", err)
	}

	if record, err := c.Get([]byte("xxxx")); err != nil {
		t.Logf("we cannot find this record now")
	} else {
		if record != nil {
			t.Fatalf("del failed, can not delete cache item\n")
		}
	}
}
