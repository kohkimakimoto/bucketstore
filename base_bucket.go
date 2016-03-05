package bucketstore

import (
	"encoding/json"
	"fmt"
	"github.com/kohkimakimoto/bucketstore/v/bolt"
	"strings"
)

// BaseBucket is a low level bucket that is used inside of bucketstore.
type BaseBucket struct {
	name  []byte
	tx    *Tx
	data  *bolt.Bucket
	index *bolt.Bucket
}

func newBaseBucket(name []byte, tx *Tx, data *bolt.Bucket, index *bolt.Bucket) *BaseBucket {
	return &BaseBucket{
		name:  name,
		tx:    tx,
		data:  data,
		index: index,
	}
}

func (b *BaseBucket) Cursor() *bolt.Cursor {
	return b.data.Cursor()
}

func (b *BaseBucket) Get(key []byte) []byte {
	return b.data.Get(key)
}

func (b *BaseBucket) Put(key []byte, value []byte) error {
	var jsonMap map[string]interface{}
	if err := json.Unmarshal(value, &jsonMap); err != nil {
		return fmt.Errorf("invalid json formatted data: %v", err)
	}

	reformattedJsonBytes, err := json.Marshal(jsonMap)
	if err != nil {
		return err
	}

	value = reformattedJsonBytes

	err = b.refreshIndex(key, jsonMap)
	if err != nil {
		return err
	}

	// save key/value pair
	if err := b.data.Put(key, value); err != nil {
		return err
	}

	return nil
}

func (b *BaseBucket) Delete(key []byte) error {
	if err := b.refreshIndex(key, nil); err != nil {
		return err
	}

	// delete key/value pair
	if err := b.data.Delete(key); err != nil {
		return err
	}

	return nil
}

func (b *BaseBucket) NextSequence() (uint64, error) {
	return b.data.NextSequence()
}

func (b *BaseBucket) ForEach(fn func(k, v []byte) error) error {
	return b.data.ForEach(fn)
}

func (b *BaseBucket) Stats() bolt.BucketStats {
	return b.data.Stats()
}

func (b *BaseBucket) Data() *bolt.Bucket {
	return b.data
}

func (b *BaseBucket) IndexCursor(propName string) *IndexCursor {
	return newIndexCursor(b, propName)
}

func (b *BaseBucket) IndexProperties() (props []string, err error) {
	// iterate indexed properties for system inspection.
	err = b.index.ForEach(func(k, v []byte) error {
		props = append(props, string(k))
		return nil
	})
	if err != nil {
		return props, err
	}

	return props, err
}

func (b *BaseBucket) refreshIndex(key []byte, jsonMap map[string]interface{}) error {
	deletedIndexBucketNames := []string{}

	// Delete existing index
	oldValue := b.data.Get(key)
	if oldValue != nil {
		// Try to unmarshal value as a json to index by it's properties.
		// If it is not a json or is an array of json. doesn't index it.
		var oldJsonMap map[string]interface{}
		if err := json.Unmarshal(oldValue, &oldJsonMap); err == nil {
			// exists indexes
			// remove them.
			for n, v := range oldJsonMap {
				indexKey := genIndexKey(v, key)
				if indexKey == nil {
					continue
				}

				indexBucket := b.getIndexBucket(n)
				if indexBucket != nil {
					err := indexBucket.Delete(indexKey)
					if err != nil {
						return err
					}

					deletedIndexBucketNames = append(deletedIndexBucketNames, n)
				}
			}
		}
	}

	// create new index
	if jsonMap != nil {
		for n, v := range jsonMap {
			if b.isIgnorePattern(n) {
				continue
			}

			indexKey := genIndexKey(v, key)
			if indexKey == nil {
				continue
			}

			indexBucket, err := b.createIndexBucketIfNotExists(n)
			if err != nil {
				return err
			}

			err = indexBucket.Put(indexKey, key)

			if err != nil {
				return err
			}
		}
	}

	// clean empty buckets
	for _, n := range deletedIndexBucketNames {
		indexBucket := b.getIndexBucket(n)
		if indexBucket != nil {
			c := indexBucket.Cursor()
			if k, _ := c.First(); k == nil {
				err := b.deleteIndexBucket(n)
				if err != nil {
					return fmt.Errorf("couldn't delete empty index bucket '%s' due to the error '%v'", n, err)
				}
			}
		}
	}

	return nil
}

func (b *BaseBucket) isIgnorePattern(propName string) bool {
	return strings.HasPrefix(propName, "_")
}

func (b *BaseBucket) getIndexBucket(propName string) *bolt.Bucket {
	// there is a bucket of index per property name.
	return b.index.Bucket([]byte(propName))
}

func (b *BaseBucket) deleteIndexBucket(propName string) error {
	return b.index.DeleteBucket([]byte(propName))
}

func (b *BaseBucket) createIndexBucketIfNotExists(propName string) (*bolt.Bucket, error) {
	return b.index.CreateBucketIfNotExists([]byte(propName))
}
