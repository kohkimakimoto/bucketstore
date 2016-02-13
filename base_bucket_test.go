package bucketstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestBaseBucketIndexProperties(t *testing.T) {
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

	// run update
	ds.Update(func(tx *Tx) error {
		// create bucket
		b, err := tx.createBaseBucketIfNotExists([]byte("test_bucket1"))
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		// put
		key := []byte("key1")
		value := []byte(`{"aaa":"bbb", "hoge": "bbbb"}`)
		err = b.Put(key, value)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		key = []byte("key2")
		value = []byte(`{"aaa":"bbb", "ccc":"bbb", "bbb": "bbbb"}`)
		err = b.Put(key, value)
		if err != nil {
			t.Errorf("should not raise error: %v", err)
		}

		props, _ := b.IndexProperties()

		if len(props) != 4 {
			t.Errorf("invalid count: %d", props)
		}

		fmt.Println("[debug] list indexes.")
		for _, p := range props {
			fmt.Println("[debug] " + p)
		}
		return nil
	})
}
