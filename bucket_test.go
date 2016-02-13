package bucketstore

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestBucketRawCRUD(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	db, err := Open(tmpFile.Name(), 0600, nil)
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test_bucket")
	bucket.PutRaw([]byte("key1"), []byte(`{"aaa":"bbb"}`))
	bucket.PutRaw([]byte("key2"), []byte(`{"aaa":"ccc"}`))

	v, err := bucket.GetRaw([]byte("key1"))
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}

	if string(v) != `{"aaa":"bbb"}` {
		t.Errorf("unmatch: %s actual: %s", `{"aaa":"bbb"}`, string(v))
	}

	err = bucket.Delete([]byte("key1"))
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}

	v, err = bucket.GetRaw([]byte("key1"))
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}

	if v != nil {
		t.Errorf("unmatch: nil actual: %s", string(v))
	}
}

func TestBucketCRUD(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	db, err := Open(tmpFile.Name(), 0600, nil)
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test_bucket")

	bucket.Put(&Item{Key: []byte("key1"), Value: []byte(`{"aaa":"bbb"}`)})
	bucket.Put(&Item{Key: []byte("key2"), Value: []byte(`{"aaa":"ccc"}`)})

	item, err := bucket.Get([]byte("key1"))
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}

	if string(item.Value) != `{"aaa":"bbb"}` {
		t.Errorf("unmatch: %s actual: %s", `{"aaa":"bbb"}`, string(item.Value))
	}

	err = bucket.Delete([]byte("key1"))
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}

	item, err = bucket.Get([]byte("key1"))
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}

	if item != nil {
		t.Errorf("unmatch: nil actual: %v", item)
	}
}

func TestBucketExits(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	db, err := Open(tmpFile.Name(), 0600, nil)
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test_bucket")

	b, err := bucket.Exists()
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	if b {
		t.Errorf("should not be true.")
	}

	bucket.PutRaw([]byte("aaaa"), []byte(`{"aaa":"bbb"}`))
	b, err = bucket.Exists()
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	if !b {
		t.Errorf("should not be false.")
	}
}

func TestBucketNextSequence(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	db, err := Open(tmpFile.Name(), 0600, nil)
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test_bucket")

	i, err := bucket.NextSequence()
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}

	fmt.Printf("[debug] next sequence: %d\n", i)
	if i != 1 {
		t.Errorf("should be 1: %d", i)
	}
}

func TestBucketRawCRUDWithTx(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	db, err := Open(tmpFile.Name(), 0600, nil)
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket("test_bucket")
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		if bucket != nil {
			t.Errorf("should be nil: %v", bucket)
		}

		bucket, err = tx.CreateBucketIfNotExists("test_bucket")
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		bucket.PutRaw([]byte("key1"), []byte(`{"aaa":"bbb"}`))
		bucket.PutRaw([]byte("key2"), []byte(`{"aaa":"ccc"}`))

		v, err := bucket.GetRaw([]byte("key1"))
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		if string(v) != `{"aaa":"bbb"}` {
			t.Errorf("unmatch: %s actual: %s", `{"aaa":"bbb"}`, string(v))
		}

		err = bucket.Delete([]byte("key1"))
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		v, err = bucket.GetRaw([]byte("key1"))
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		if v != nil {
			t.Errorf("unmatch: nil actual: %s", string(v))
		}

		return nil
	})
}

func TestBucketRollbackWithTx(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	db, err := Open(tmpFile.Name(), 0600, nil)
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket("test_bucket")
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		if bucket != nil {
			t.Errorf("should be nil: %v", bucket)
		}

		bucket, err = tx.CreateBucketIfNotExists("test_bucket")
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		bucket.PutRaw([]byte("key1"), []byte(`{"aaa":"bbb"}`))
		bucket.PutRaw([]byte("key2"), []byte(`{"aaa":"ccc"}`))

		// rollback
		return errors.New("in order to rallback test")
	})

	// should be commited.
	bucket := db.Bucket("test_bucket")
	v, err := bucket.GetRaw([]byte("key1"))
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}

	if v != nil {
		t.Errorf("should be nil: %v", v)
	}
}
