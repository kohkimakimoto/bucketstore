package bucketstore

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestIndexCursorSeek(t *testing.T) {
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

	fmt.Printf("[debug] test database: %s\n", ds.Path())

	// input data
	ds.Update(func(tx *Tx) error {
		b, err := tx.createBaseBucketIfNotExists([]byte("people"))
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key := Uint64ToBytes(1)
		json := []byte(`{"name": "kohki", "age": 35, "male": true}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key = Uint64ToBytes(2)
		json = []byte(`{"name": "kohki2", "age": 30, "male": true}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key = Uint64ToBytes(3)
		json = []byte(`{"name": "kohki3", "age": 22, "male": false}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key = Uint64ToBytes(14)
		json = []byte(`{"name": "kohki4", "age": 1, "male": false}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		return nil
	})

	ds.View(func(tx *Tx) error {
		b, _ := tx.baseBucket([]byte("people"))
		c := b.IndexCursor("name")

		// seek
		i := c.SeekFirst(ValueTypeString, []byte("kohki"))
		k, v := i.Data()

		fmt.Printf("[debug] key=%d, value=%s\n", BytesToUint64(k), v)
		if BytesToUint64(k) != 1 {
			t.Errorf("invalid key %v", k)
		}

		// get
		i = c.Get("kohki")
		k, v = i.Data()
		fmt.Printf("[debug] key=%d, value=%s\n", BytesToUint64(k), v)
		if BytesToUint64(k) != 1 {
			t.Errorf("invalid key %v", k)
		}

		return nil
	})

	ds.Close()

}

func TestIndexCursorIterating(t *testing.T) {
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

	fmt.Printf("[debug] test database: %s\n", ds.Path())

	// input data
	ds.Update(func(tx *Tx) error {
		b, err := tx.createBaseBucketIfNotExists([]byte("people"))
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key := Uint64ToBytes(1)
		json := []byte(`{"name": "kohki", "age": 35, "male": true}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key = Uint64ToBytes(2)
		json = []byte(`{"name": "kohki2", "age": 30, "male": true}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key = Uint64ToBytes(3)
		json = []byte(`{"name": "kohki3", "age": 22, "male": false}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key = Uint64ToBytes(14)
		json = []byte(`{"name": "kohki4", "age": 1, "male": false}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		return nil
	})

	ds.View(func(tx *Tx) error {
		b, _ := tx.baseBucket([]byte("people"))

		fmt.Println("[debug] list by name")
		c := b.IndexCursor("name")
		for i := c.First(); i != nil; i = c.Next() {
			k, v := i.Data()
			fmt.Printf("[debug] key=%d, value=%s\n", BytesToUint64(k), v)
		}

		fmt.Println("[debug] list by age")
		c = b.IndexCursor("age")
		for i := c.First(); i != nil; i = c.Next() {
			k, v := i.Data()
			fmt.Printf("[debug] key=%d, value=%s\n", BytesToUint64(k), v)
		}

		fmt.Println("[debug] list by age desc")
		c = b.IndexCursor("age")
		for i := c.Last(); i != nil; i = c.Prev() {
			k, v := i.Data()
			fmt.Printf("[debug] key=%d, value=%s\n", BytesToUint64(k), v)
		}

		return nil
	})

	ds.Close()
}

func TestIndexCursorPrefixScans(t *testing.T) {
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

	fmt.Printf("[debug] test database: %s\n", ds.Path())

	// input data
	ds.Update(func(tx *Tx) error {
		b, err := tx.createBaseBucketIfNotExists([]byte("people"))
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key := Uint64ToBytes(1)
		json := []byte(`{"name": "kohki", "age": 35, "male": true}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key = Uint64ToBytes(2)
		json = []byte(`{"name": "kohki2", "age": 30, "male": true}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key = Uint64ToBytes(3)
		json = []byte(`{"name": "kohki3", "age": 22, "male": false}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key = Uint64ToBytes(4)
		json = []byte(`{"name": "hoge", "age": 1, "male": false}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key = Uint64ToBytes(5)
		json = []byte(`{"name": "llll", "age": 1, "male": false}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key = Uint64ToBytes(6)
		json = []byte(`{"name": 12345, "age": 1, "male": false}`)
		err = b.Put(key, json)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		return nil
	})

	ds.View(func(tx *Tx) error {
		b, _ := tx.baseBucket([]byte("people"))

		fmt.Println("[debug] list by name that has prefix 'kohki' ")
		c := b.IndexCursor("name")

		prefix := []byte("kohki")
		for i := c.SeekFirst(ValueTypeString, prefix); i != nil && i.ValueType() == ValueTypeString && bytes.HasPrefix(i.MustValueBytes(), prefix); i = c.Next() {
			k, v := i.Data()
			fmt.Printf("[debug] key=%d, value=%s\n", BytesToUint64(k), v)
		}

		return nil
	})

	ds.Close()
}
