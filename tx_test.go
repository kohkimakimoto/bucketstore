package bucketstore

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestTx(t *testing.T) {
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

		return nil
	})
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}

}
