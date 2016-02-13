package bucketstore

import (
	"bytes"
	"fmt"
	"testing"
)

func TestUint64ToBytes(t *testing.T) {
	b := Uint64ToBytes(256)

	if bytes.Compare(b, []byte{0, 0, 0, 0, 0, 0, 1, 0}) != 0 {
		t.Errorf("unexpected result %v", b)
	}
}

func TestBytesToUint64(t *testing.T) {
	i := BytesToUint64([]byte{0, 0, 0, 0, 0, 0, 1, 0})

	if i != 256 {
		t.Errorf("unexpected result %v", i)
	}
}

func TestStringToBytes(t *testing.T) {
	b := StringToBytes("a")

	if bytes.Compare(b, []byte{97}) != 0 {
		t.Errorf("unexpected result %v", b)
	}
}

func TestGenIndexKey(t *testing.T) {
	b := genIndexKey(true, []byte("100"))
	fmt.Printf("[debug] generated key=%v\n", b)

	b = genIndexKey("kohkimakimoto", []byte("101"))
	fmt.Printf("[debug] generated key=%v\n", b)

	b = genIndexKey(float64(-10.5), []byte("101"))
	fmt.Printf("[debug] generated key=%v\n", b)
}

func TestGenIndexFilter(t *testing.T) {
	b, _ := genIndexFilter(true)
	fmt.Printf("[debug] generated filter=%v\n", b)

	b, _ = genIndexFilter("kohkimakimoto")
	fmt.Printf("[debug] generated filter=%v\n", b)

	b, _ = genIndexFilter(float64(-10.5))
	fmt.Printf("[debug] generated filter=%v\n", b)
}

func TestGetValueFromIndexKey(t *testing.T) {
	b := genIndexKey("kohkimakimoto", []byte("101"))
	bb, err := getValueFromIndexKey(b)
	if err != nil {
		t.Errorf("should not raise error")
	}

	if string(bb) != "kohkimakimoto" {
		t.Errorf("invalid value : %s %v", string(bb), bb)
	}

	b = genIndexKey(true, []byte("101"))
	bb, err = getValueFromIndexKey(b)
	if err != nil {
		t.Errorf("should not raise error")
	}

	if bb[0] != byte(1) {
		t.Errorf("invalid value : %s %v", string(bb), bb)
	}

	b = genIndexKey(false, []byte("101"))
	bb, err = getValueFromIndexKey(b)
	if err != nil {
		t.Errorf("should not raise error")
	}

	if bb[0] != byte(0) {
		t.Errorf("invalid value : %s %v", string(bb), bb)
	}

	b = genIndexKey(123, []byte("101"))
	bb, err = getValueFromIndexKey(b)
	if err != nil {
		t.Errorf("should not raise error")
	}
	if int(BytesToFloat64(bb)) != 123 {
		t.Errorf("invalid value : %s %v", string(bb), bb)
	}
}

func TestToIndexedBytes(t *testing.T) {
	_, b := toIndexedBytes("a")
	if b != ValueTypeString {
		t.Errorf("invalid type : %v", b)
	}

	_, b = toIndexedBytes(123.2)
	if b != ValueTypeFloat64 {
		t.Errorf("invalid type : %v", b)
	}

	_, b = toIndexedBytes(123)
	if b != ValueTypeFloat64 {
		t.Errorf("invalid type : %v", b)
	}
}