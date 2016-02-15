package bucketstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

func Uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func BytesToFloat64(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	return math.Float64frombits(bits)
}

func StringToBytes(v string) []byte {
	return []byte(v)
}

const (
	ValueTypeBool    = 0x01
	ValueTypeString  = 0x02
	ValueTypeFloat64 = 0x03
	ValueTypeNil     = 0x04
	valueTypeNoIndex = 0x00
)

//
// # Index specification.
//
// ## index key structure
//
// The following byte slice is index key structure.
//
//   <valueType> + <value> + "0x00 0xFF" + <key>
//
// "0x00 0xFF" is a separator this spec reason is the below.
//   0x00 is least byte pattern to match prefix string by seek.
//   0xFF is not in the utf8 code.
//
// Example: true
//  0x01 0x01 0x00 0xFF <key>
//
// Example: "abc"
//  0x02 0x61 0x62 0x63 0x00 0xFF <key>
//
// Example: 1
//  0x03 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01 0x00 0xFF <key>
//
// ## Description
//
//  To search value using index, you can user IndexCursor that is the low level API.
//  This cursor can move to the fist and last point matching bytes pattern in the specific property.
//
//  Example1)
//    If you want to search items with "ab" prefix. IndexCoursor seek by the following bytes pattern.
//
//      0x02 0x61 0x62 0x00 0xFF
//
//    This bytes the below structure.
//
//      <valueType> + <value> + "0x00 0xFF"
//
//    If you have the "abc" value in your database like the above example this pattern is below.
//
//      0x02 0x61 0x62 0x63 0x00 0xFF
//
//    IndexCursor bytes pattern is "0x02 0x61 0x62 0x00 0xFF", so It moves to before "abc" "abd" "abb" etc...
//    And It walks using "Next" method until not to match the prefix .
//
// Example2)
//   If you want to search items with "ab" prefix descending order. IndexCoursor seek by the following bytes pattern.
//
//      0x02 0x61 0x62 0xFF 0xFF
//
//    This bytes the below structure.
//
//      <valueType> + <value> + "0xFF 0xFF"
//
//    If you have the "abc" value in your database like the above example this pattern is below.
//
//      0x02 0x61 0x62 0x63 0x00 0xFF
//
//    IndexCursor bytes pattern is "0x02 0x61 0x62 0xFF 0xFF", so It moves to after "abc" "abd" "abb" etc...
//    For instance, cursor moves to "aca" that is the fist point after all matching pattern space.
//    You should move one previous position that is the last matching point.
//    It walks using "Prev" method until not to match the prefix.
//

const (
	sep1         = 0x00
	sep2         = 0xFF
	maxValueSize = 255
)

func genIndexKey(value interface{}, key []byte) []byte {
	valueBytes, valueTypeByte := toIndexedBytes(value)
	if valueTypeByte == valueTypeNoIndex {
		// does not index.
		return nil
	}

	// only support indexing in max 255 bytes.
	size := len(valueBytes)
	if valueTypeByte == ValueTypeString && size > maxValueSize {
		valueBytes = valueBytes[:maxValueSize]
		size = len(valueBytes)
	}

	return append(append(append([]byte{valueTypeByte}, valueBytes[:]...), sep1, sep2), key[:]...)
}

func genIndexPrefixForSeekFirst(valueType byte, value []byte) []byte {

	// only support indexing in max 255 bytes.
	if valueType == ValueTypeString && len(value) > maxValueSize {
		value = value[:maxValueSize]
	}

	return append(append([]byte{valueType}, value[:]...), sep1, sep2)
}

// genIndexPrefixForSeekLast generates prefix bytes pattern to find item
// that is one more biggger than specified one.
func genIndexPrefixForSeekLast(valueType byte, value []byte) []byte {

	// only support indexing in max 255 bytes.
	if valueType == ValueTypeString && len(value) > maxValueSize {
		value = value[:maxValueSize]
	}

	return append(append([]byte{valueType}, value[:]...), 0xFF, sep2)
}

func genIndexFilter(value interface{}) ([]byte, byte) {
	valueBytes, valueTypeByte := toIndexedBytes(value)
	if valueTypeByte == valueTypeNoIndex {
		// does not index.
		return nil, valueTypeNoIndex
	}
	return genIndexPrefixForSeekFirst(valueTypeByte, valueBytes), valueTypeByte
}

func getValueFromIndexKey(indexKey []byte) ([]byte, error) {
	// get value type
	switch indexKey[0] {

	case ValueTypeBool:
		return indexKey[1:2], nil
	case ValueTypeString:
		i := bytes.Index(indexKey[1:], []byte{sep1, sep2})
		if i == -1 {
			return nil, fmt.Errorf("got a illegal formatted key %v", indexKey)
		}
		return indexKey[1:i+1], nil
	case ValueTypeFloat64:
		return indexKey[1:9], nil
	case ValueTypeNil:
		return nil, nil
	}

	return nil, fmt.Errorf("got a illegal formatted key %v", indexKey)
}

func toIndexedBytes(value interface{}) ([]byte, byte) {
	// https://golang.org/pkg/encoding/json/#Unmarshal
	switch converted := value.(type) {
	case bool:
		// JSON boolean
		if converted {
			return []byte{1}, ValueTypeBool
		} else {
			return []byte{0}, ValueTypeBool
		}
	case string:
		// JSON string
		return []byte(converted), ValueTypeString
	case float64:
		// JSON number
		// http://stackoverflow.com/questions/22491876/convert-byte-array-uint8-to-float64-in-golang
		bits := math.Float64bits(converted)
		bytes := make([]byte, 8)
		binary.BigEndian.PutUint64(bytes, bits)
		return bytes, ValueTypeFloat64
	case int:
		// JSON number
		c := float64(converted)

		bits := math.Float64bits(c)
		bytes := make([]byte, 8)
		binary.BigEndian.PutUint64(bytes, bits)
		return bytes, ValueTypeFloat64
	case int64:
		// JSON number
		c := float64(converted)

		bits := math.Float64bits(c)
		bytes := make([]byte, 8)
		binary.BigEndian.PutUint64(bytes, bits)
		return bytes, ValueTypeFloat64
	case nil:
		// JSON null
		return nil, ValueTypeNil
	case []interface{}:
		// JSON array
		// not support indexing...
		return nil, valueTypeNoIndex
	case map[string]interface{}:
		// JSON object
		// not support indexing...
		return nil, valueTypeNoIndex
	default:
		return nil, valueTypeNoIndex
	}
}
