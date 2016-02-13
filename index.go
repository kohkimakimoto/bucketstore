package bucketstore

type Index struct {
	bucket *BaseBucket
	// key is a index key for searching.
	key []byte
	// ref is a key of actual key/value pair.
	ref []byte
}

func newIndex(bucket *BaseBucket, key []byte, ref []byte) *Index {
	return &Index{
		bucket: bucket,
		key:    key,
		ref:    ref,
	}
}

func (idx *Index) Data() (key []byte, value []byte) {
	return idx.ref, idx.bucket.Get(idx.ref)
}

func (idx *Index) ValueType() byte {
	return idx.key[0]
}

func (idx *Index) ValueBytes() ([]byte, error) {
	return getValueFromIndexKey(idx.key)
}

func (idx *Index) MustValueBytes() []byte {
	b, err := getValueFromIndexKey(idx.key)
	if err != nil {
		panic(err)
	}

	return b
}
