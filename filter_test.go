package bucketstore

import (
	"io/ioutil"
	"os"
	"testing"
	"fmt"
)

func TestFilterOrderFilter(t *testing.T) {
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
	bucket.PutRaw([]byte("key1"), []byte(`{"aaa": "bbb1"}`))
	bucket.PutRaw([]byte("key2"), []byte(`{"aaa": "bbb2"}`))
	bucket.PutRaw([]byte("key3"), []byte(`{"aaa": "bbb3"}`))
	bucket.PutRaw([]byte("key4"), []byte(`{"aaa": "bbb4"}`))
	bucket.PutRaw([]byte("key5"), []byte(`{"aaa": "bbb5"}`))

	q := bucket.Query()
	q.Filter = &OrderByFilter{
		OrderBy: OrderByDesc,
	}
	items, err := q.AsList()

	if string(items[0].Key) != "key5" {
		t.Errorf("unmatch: %s", string(items[0].Key))
	}

	if string(items[1].Key) != "key4" {
		t.Errorf("unmatch: %s", string(items[1].Key))
	}
}

func TestFilterPrefixFilter(t *testing.T) {
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
	bucket.PutRaw([]byte("key1"), []byte(`{"aaa": "bbb1"}`))
	bucket.PutRaw([]byte("key2"), []byte(`{"aaa": "bbb2"}`))
	bucket.PutRaw([]byte("akey3"), []byte(`{"aaa": "ccc3"}`))
	bucket.PutRaw([]byte("key4"), []byte(`{"aaa": "aaa2"}`))
	bucket.PutRaw([]byte("akey5"), []byte(`{"aaa": "bbb5"}`))

	q := bucket.Query()
	q.Filter = &KeyPrefixFilter{
		Prefix: []byte("key"),
	}
	items, err := q.AsList()

	if len(items) != 3 {
		t.Errorf("invalid limit: %d", len(items))
	}

	if string(items[2].Key) != "key4" {
		t.Errorf("unmatch: %s", string(items[2].Key))
	}
}

func TestFilterRangeFilter(t *testing.T) {
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
	bucket.PutRaw([]byte("1990-01-01T00:00:00Z"), []byte(`{"aaa": "bbb1"}`))
	bucket.PutRaw([]byte("1990-01-02T00:00:00Z"), []byte(`{"aaa": "bbb2"}`))
	bucket.PutRaw([]byte("1990-01-03T00:00:00Z"), []byte(`{"aaa": "ccc3"}`))
	bucket.PutRaw([]byte("1991-01-01T00:00:00Z"), []byte(`{"aaa": "aaa2"}`))
	bucket.PutRaw([]byte("1993-01-01T00:00:00Z"), []byte(`{"aaa": "bbb5"}`))

	bucket.PutRaw([]byte("1980-01-01T00:00:00Z"), []byte(`{"aaa": "aaa2"}`))
	bucket.PutRaw([]byte("1980-02-01T00:00:00Z"), []byte(`{"aaa": "bbb5"}`))

	q := bucket.Query()
	q.Filter = &KeyRangeFilter{
		Min:     []byte("1990-01-01T00:00:00Z"),
		Max:     []byte("1991-01-01T00:00:00Z"),
		OrderBy: OrderByDesc,
	}
	items, err := q.AsList()

	if len(items) != 4 {
		t.Errorf("invalid limit: %d", len(items))
	}

	if string(items[3].Key) != "1990-01-01T00:00:00Z" {
		t.Errorf("unmatch: %s", string(items[3].Key))
	}
}

func TestFilterPropertyValuePrefixFilter(t *testing.T) {
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
	bucket.PutRaw([]byte("1990-01-01T00:00:00Z"), []byte(`{"aaa": "bbb1"}`))
	bucket.PutRaw([]byte("1990-01-02T00:00:00Z"), []byte(`{"aaa": "bbb2"}`))
	bucket.PutRaw([]byte("1990-01-03T00:00:00Z"), []byte(`{"aaa": "ccc3"}`))
	bucket.PutRaw([]byte("1991-01-01T00:00:00Z"), []byte(`{"aaa": "aaa2"}`))
	bucket.PutRaw([]byte("1993-01-01T00:00:00Z"), []byte(`{"aaa": "bbb5"}`))

	bucket.PutRaw([]byte("1980-01-01T00:00:00Z"), []byte(`{"aaa": "aaa2"}`))
	bucket.PutRaw([]byte("1980-02-01T00:00:00Z"), []byte(`{"aaa": "bbb0"}`))

	q := bucket.Query()
	q.Filter = &PropValuePrefixFilter{
		Property: "aaa",
		Prefix:   "bbb",
	}
	items, err := q.AsList()

	if len(items) != 4 {
		t.Errorf("invalid limit: %d", len(items))
	}

	if string(items[0].Key) != "1980-02-01T00:00:00Z" {
		t.Errorf("unmatch: %s", string(items[0].Key))
	}
}

func TestFilterPropertyValueRangeFilter(t *testing.T) {
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
	bucket.PutRaw([]byte("key1"), []byte(`{"aaa": "bbb1", "date": "1991-01-01T00:00:00Z"}`))
	bucket.PutRaw([]byte("key2"), []byte(`{"aaa": "bbb2", "date": "1991-02-01T00:00:00Z"}`))
	bucket.PutRaw([]byte("key3"), []byte(`{"aaa": "ccc3", "date": "1990-03-01T00:00:00Z"}`))
	bucket.PutRaw([]byte("key4"), []byte(`{"aaa": "aaa2", "date": "1990-04-01T00:00:00Z"}`))
	bucket.PutRaw([]byte("key5"), []byte(`{"aaa": "bbb5", "date": "1990-05-01T00:00:00Z"}`))

	bucket.PutRaw([]byte("key6"), []byte(`{"aaa": "aaa2", "date": "1995-01-01T00:00:00Z"}`))
	bucket.PutRaw([]byte("key7"), []byte(`{"aaa": "bbb0", "date": "1995-02-01T00:00:00Z"}`))

	// asc
	q := bucket.Query()
	q.Filter = &PropValueRangeFilter{
		Property: "date",
		Min:      "1991-01-01T00:00:00Z",
		Max:      "1995-01-01T00:00:00Z",
	}

	items, err := q.AsList()

	if len(items) != 3 {
		t.Errorf("invalid limit: %d", len(items))
	}

	if string(items[0].Key) != "key1" {
		t.Errorf("unmatch: %s", string(items[0].Key))
	}

	// desc
	q = bucket.Query()
	q.Filter = &PropValueRangeFilter{
		Property: "date",
		Min:      "1991-01-01T00:00:00Z",
		Max:      "1995-01-01T00:00:00Z",
		OrderBy:    OrderByDesc,
	}

	items, err = q.AsList()

	if len(items) != 3 {
		t.Errorf("invalid limit: %d", len(items))
	}

	if string(items[0].Key) != "key6" {
		t.Errorf("unmatch: %s", string(items[0].Key))
	}
}

func TestFilterPropertyValueRangeFilter2(t *testing.T) {
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
	bucket.PutRaw([]byte("key1"), []byte(`{"aaa": "bbb1", "num": 123}`))
	bucket.PutRaw([]byte("key2"), []byte(`{"aaa": "bbb2", "num": 133}`))
	bucket.PutRaw([]byte("key3"), []byte(`{"aaa": "ccc3", "num": 153}`))
	bucket.PutRaw([]byte("key4"), []byte(`{"aaa": "aaa2", "num": 163}`))
	bucket.PutRaw([]byte("key5"), []byte(`{"aaa": "bbb5", "num": 223}`))
	bucket.PutRaw([]byte("key6"), []byte(`{"aaa": "aaa2", "num": 113}`))
	bucket.PutRaw([]byte("key7"), []byte(`{"aaa": "aaa2", "num": 163}`))
	bucket.PutRaw([]byte("key8"), []byte(`{"aaa": "bbb0", "num": 1393}`))
	bucket.PutRaw([]byte("key9"), []byte(`{"aaa": "bbb0", "num": 133}`))

	// asc
	q := bucket.Query()
	q.Filter = &PropValueRangeFilter{
		Property: "num",
		Min:      133,
		Max:      163,
	}

	items, err := q.AsList()

	if len(items) != 5 {
		t.Errorf("invalid limit: %d", len(items))
	}

	if string(items[0].Key) != "key2" {
		t.Errorf("unmatch: %s", string(items[0].Key))
	}

	// desc
	q = bucket.Query()
	q.Filter = &PropValueRangeFilter{
		Property: "num",
		Min:      133,
		Max:      163,
		OrderBy:    OrderByDesc,
	}

	items, err = q.AsList()

	fmt.Println(string(items[0].Key), string(items[1].Key))

	if len(items) != 5 {
		t.Errorf("invalid limit: %d", len(items))
	}

	if string(items[0].Key) != "key7" {
		t.Errorf("unmatch: %s", string(items[0].Key))
	}
}

func TestFilterPropertyValueMatchFilter(t *testing.T) {
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
	bucket.PutRaw([]byte("key1"), []byte(`{"aaa": "bbb1", "num": 123}`))
	bucket.PutRaw([]byte("key2"), []byte(`{"aaa": "bbb2", "num": 133}`))
	bucket.PutRaw([]byte("key3"), []byte(`{"aaa": "bbb2", "num": 153}`))
	bucket.PutRaw([]byte("key4"), []byte(`{"aaa": "bbb3", "num": 163}`))
	bucket.PutRaw([]byte("key5"), []byte(`{"aaa": "bbb2", "num": 223}`))
	bucket.PutRaw([]byte("key6"), []byte(`{"aaa": "bbb5", "num": 113}`))
	bucket.PutRaw([]byte("key7"), []byte(`{"aaa": "aaa2", "num": 163}`))
	bucket.PutRaw([]byte("key8"), []byte(`{"aaa": "bbb0", "num": 1393}`))
	bucket.PutRaw([]byte("key9"), []byte(`{"aaa": "bbb2", "num": 133}`))

	// asc
	q := bucket.Query()
	q.Filter = &PropValueMatchFilter{
		Property: "aaa",
		Match: "bbb2",
	}

	items, err := q.AsList()

	if len(items) != 4 {
		t.Errorf("invalid limit: %d", len(items))
	}

	if string(items[0].Key) != "key2" {
		t.Errorf("unmatch: %s", string(items[0].Key))
	}

	// desc
	q = bucket.Query()
	q.Filter = &PropValueMatchFilter{
		Property: "aaa",
		Match: "bbb2",
		OrderBy: OrderByDesc,
	}

	items, err = q.AsList()

	if len(items) != 4 {
		t.Errorf("invalid limit: %d", len(items))
	}

	if string(items[0].Key) != "key9" {
		t.Errorf("unmatch: %s", string(items[0].Key))
	}
}



