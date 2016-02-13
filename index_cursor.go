package bucketstore

import (
	"bytes"
	"github.com/kohkimakimoto/bucketstore/vendor/bolt"
)

type IndexCursor struct {
	bucket      *BaseBucket
	propName    string
	indexBucket *bolt.Bucket
	cursor      *bolt.Cursor
}

func newIndexCursor(b *BaseBucket, propName string) *IndexCursor {
	var cursor *bolt.Cursor
	indexBucket := b.getIndexBucket(propName)
	if indexBucket != nil {
		cursor = indexBucket.Cursor()
	}

	return &IndexCursor{
		bucket:      b,
		indexBucket: indexBucket,
		propName:    propName,
		cursor:      cursor,
	}
}

func (ic *IndexCursor) Bucket() *BaseBucket {
	return ic.bucket
}

func (ic *IndexCursor) First() *Index {
	if ic.cursor == nil {
		return nil
	}

	k, v := ic.cursor.First()
	if k == nil {
		return nil
	}

	return newIndex(ic.bucket, k, v)
}

func (ic *IndexCursor) Last() *Index {
	if ic.cursor == nil {
		return nil
	}

	k, v := ic.cursor.Last()
	if k == nil {
		return nil
	}

	return newIndex(ic.bucket, k, v)
}

func (ic *IndexCursor) Next() *Index {
	if ic.cursor == nil {
		return nil
	}

	k, v := ic.cursor.Next()
	if k == nil {
		return nil
	}

	return newIndex(ic.bucket, k, v)
}

func (ic *IndexCursor) Prev() *Index {
	if ic.cursor == nil {
		return nil
	}

	k, v := ic.cursor.Prev()
	if k == nil {
		return nil
	}

	return newIndex(ic.bucket, k, v)
}

// SeekFirst moves cursor to fist key matching `seek` prefix
func (ic *IndexCursor) SeekFirst(valueType byte, seek []byte) *Index {
	if ic.cursor == nil {
		return nil
	}

	filter := genIndexPrefixForSeekFirst(valueType, seek)

	k, v := ic.cursor.Seek(filter)
	if k == nil {
		return nil
	}

	idx := newIndex(ic.bucket, k, v)

	// check value type
	if idx.ValueType() != valueType {
		return nil
	}

	return newIndex(ic.bucket, k, v)
}

// SeekFirst moves cursor to last key matching `seek` prefix
func (ic *IndexCursor) SeekLast(valueType byte, seek []byte) *Index {
	if ic.cursor == nil {
		return nil
	}

	filter := genIndexPrefixForSeekLast(valueType, seek)

	// seek one more biggger item than specified one.
	ic.cursor.Seek(filter)
	k, v := ic.cursor.Prev()
	if k == nil {
		return nil
	}

	idx := newIndex(ic.bucket, k, v)

	// check value type
	if idx.ValueType() != valueType {
		return nil
	}

	return newIndex(ic.bucket, k, v)
}

func (ic *IndexCursor) Get(value interface{}) *Index {
	if ic.cursor == nil {
		return nil
	}

	filter, _ := genIndexFilter(value)

	k, v := ic.cursor.Seek(filter)
	if k == nil {
		return nil
	}

	// checks exactly the same value.
	if !bytes.Equal(k, genIndexKey(value, v)) {
		return nil
	}

	return newIndex(ic.bucket, k, v)
}
