package bucketstore

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestQueryAsSingle(t *testing.T) {
	// setup database
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	ds, err := Open(tmpFile.Name(), 0600, nil)
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer ds.Close()

	bucket := ds.Bucket("test_bucket")
	bucket.PutRaw([]byte("key1"), []byte(`{"aaa": "aaabbb", "bbb": 11}`))
	bucket.PutRaw([]byte("key2"), []byte(`{"aaa": "aaabbbccc", "bbb": 122}`))
	bucket.PutRaw([]byte("key3"), []byte(`{"aaa": "aaabbbddd", "bbb": 44}`))
	bucket.PutRaw([]byte("key4"), []byte(`{"aaa": "aaabbbeee", "bbb": 12}`))
	bucket.PutRaw([]byte("key5"), []byte(`{"aaa": "aaabbbccc", "bbb": 1}`))
	bucket.PutRaw([]byte("key6"), []byte(`{"aaa": "aaabbbfff", "bbb": 8}`))

	q := bucket.Query()
	item, err := q.AsSingle()
	if string(item.Key) != "key1" {
		t.Errorf("unmatch: %s", string(item.Key))
	}
}

func TestQueryAsList(t *testing.T) {
	// setup database
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	ds, err := Open(tmpFile.Name(), 0600, nil)
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer ds.Close()

	bucket := ds.Bucket("test_bucket")
	bucket.PutRaw([]byte("key1"), []byte(`{"aaa": "aaabbb", "bbb": 11}`))
	bucket.PutRaw([]byte("key2"), []byte(`{"aaa": "aaabbbccc", "bbb": 122}`))
	bucket.PutRaw([]byte("key3"), []byte(`{"aaa": "aaabbbddd", "bbb": 44}`))
	bucket.PutRaw([]byte("key4"), []byte(`{"aaa": "aaabbbeee", "bbb": 12}`))
	bucket.PutRaw([]byte("key5"), []byte(`{"aaa": "aaabbbccc", "bbb": 1}`))
	bucket.PutRaw([]byte("key6"), []byte(`{"aaa": "aaabbbfff", "bbb": 8}`))

	q := bucket.Query()
	items, err := q.AsList()
	if string(items[0].Key) != "key1" {
		t.Errorf("unmatch: %s", string(items[0].Key))
	}
}

func TestQueryAsListLimit(t *testing.T) {
	// setup database
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	ds, err := Open(tmpFile.Name(), 0600, nil)
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer ds.Close()

	bucket := ds.Bucket("test_bucket")
	bucket.PutRaw([]byte("key1"), []byte(`{"aaa": "aaabbb", "bbb": 11}`))
	bucket.PutRaw([]byte("key2"), []byte(`{"aaa": "aaabbbccc", "bbb": 122}`))
	bucket.PutRaw([]byte("key3"), []byte(`{"aaa": "aaabbbddd", "bbb": 44}`))
	bucket.PutRaw([]byte("key4"), []byte(`{"aaa": "aaabbbeee", "bbb": 12}`))
	bucket.PutRaw([]byte("key5"), []byte(`{"aaa": "aaabbbccc", "bbb": 1}`))
	bucket.PutRaw([]byte("key6"), []byte(`{"aaa": "aaabbbfff", "bbb": 8}`))

	q := bucket.Query()
	q.Limit = 3
	items, err := q.AsList()

	if len(items) != 3 {
		t.Errorf("invalid limit: %d", len(items))
	}
}

func TestQueryAsListOffsetLimit(t *testing.T) {
	// setup database
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	ds, err := Open(tmpFile.Name(), 0600, nil)
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer ds.Close()

	bucket := ds.Bucket("test_bucket")
	bucket.PutRaw([]byte("key1"), []byte(`{"aaa": "aaabbb", "bbb": 11}`))
	bucket.PutRaw([]byte("key2"), []byte(`{"aaa": "aaabbbccc", "bbb": 122}`))
	bucket.PutRaw([]byte("key3"), []byte(`{"aaa": "aaabbbddd", "bbb": 44}`))
	bucket.PutRaw([]byte("key4"), []byte(`{"aaa": "aaabbbeee", "bbb": 12}`))
	bucket.PutRaw([]byte("key5"), []byte(`{"aaa": "aaabbbccc", "bbb": 1}`))
	bucket.PutRaw([]byte("key6"), []byte(`{"aaa": "aaabbbfff", "bbb": 8}`))

	q := bucket.Query()
	q.Offset = 2
	q.Limit = 3
	items, err := q.AsList()

	if len(items) != 3 {
		t.Errorf("invalid limit: %d", len(items))
	}

	if string(items[0].Key) != "key3" {
		t.Errorf("unmatch: %s", string(items[0].Key))
	}
}
