package bucketstore

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestDBOpen(t *testing.T) {
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

	// check path
	p := ds.Path()
	if p == "" {
		t.Errorf("could not get database path")
	}

	// check go string
	gs := ds.GoString()
	if gs == "" {
		t.Errorf("could not get database go string")
	}

	// check string
	str := ds.String()
	if str == "" {
		t.Errorf("could not get database string")
	}

	// check connection
	conn := ds.Conn()
	if conn == nil {
		t.Errorf("could not get database connection")
	}
}

func TestDBBegin(t *testing.T) {
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

	tx, err := ds.Begin(true)
	if err != nil {
		t.Errorf("should not raise error: %v", err)
	}

	tx.Commit()
}

func TestDBBucket(t *testing.T) {
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
	if bucket == nil {
		t.Errorf("could not get bucket.")
	}
}
