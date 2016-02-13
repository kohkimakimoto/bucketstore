package bucketstore

type Bucket struct {
	name      string
	datastore *DB

	baseBucket *BaseBucket
}

func newBucket(name string, ds *DB, baseBucket *BaseBucket) *Bucket {
	return &Bucket{
		name:       name,
		datastore:  ds,
		baseBucket: baseBucket,
	}
}

func (bucket *Bucket) Get(key []byte) (*Item, error) {
	v, err := bucket.GetRaw(key)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	return &Item{Key: key, Value: v}, nil
}

func (bucket *Bucket) GetRaw(key []byte) (value []byte, err error) {
	if bucket.baseBucket != nil {
		baseBucket := bucket.baseBucket

		value = baseBucket.Get(key)
		return value, err
	}

	err = bucket.datastore.View(func(tx *Tx) error {
		baseBucket, err := tx.baseBucket([]byte(bucket.name))
		if err != nil {
			return err
		}

		if baseBucket == nil {
			return nil
		}

		value = baseBucket.Get(key)
		return nil
	})

	return value, err
}

func (bucket *Bucket) Put(item *Item) (err error) {
	return bucket.PutRaw(item.Key, item.Value)
}

func (bucket *Bucket) PutRaw(key []byte, value []byte) (err error) {
	if bucket.baseBucket != nil {
		baseBucket := bucket.baseBucket

		return baseBucket.Put(key, value)
	}

	return bucket.datastore.Update(func(tx *Tx) error {
		baseBucket, err := tx.createBaseBucketIfNotExists([]byte(bucket.name))
		if err != nil {
			return err
		}

		return baseBucket.Put(key, value)
	})
}

func (bucket *Bucket) Delete(key []byte) error {
	if bucket.baseBucket != nil {
		baseBucket := bucket.baseBucket

		return baseBucket.Delete(key)
	}

	return bucket.datastore.Update(func(tx *Tx) error {
		baseBucket, err := tx.baseBucket([]byte(bucket.name))
		if err != nil {
			return err
		}

		if baseBucket == nil {
			return nil
		}

		return baseBucket.Delete(key)
	})
}

func (bucket *Bucket) Query() *Query {
	return newQuery(bucket)
}

func (bucket *Bucket) Exists() (bool, error) {
	if bucket.baseBucket != nil {
		return true, nil
	}

	var ret bool
	err := bucket.datastore.View(func(tx *Tx) error {
		baseBucket, err := tx.baseBucket([]byte(bucket.name))
		if err != nil {
			return err
		}

		if baseBucket != nil {
			ret = true
		} else {
			ret = false
		}

		return nil
	})

	if err != nil {
		return false, err
	}

	return ret, nil
}

func (bucket *Bucket) NextSequence() (uint64, error) {
	if bucket.baseBucket != nil {
		baseBucket := bucket.baseBucket

		return baseBucket.data.NextSequence()
	}

	var next uint64
	err := bucket.datastore.Update(func(tx *Tx) error {
		baseBucket, err := tx.createBaseBucketIfNotExists([]byte(bucket.name))
		if err != nil {
			return err
		}

		n, err := baseBucket.NextSequence()
		if err != nil {
			return err
		}

		next = n
		return nil
	})

	return next, err
}

func (bucket *Bucket) BaseBucket() *BaseBucket {
	return bucket.baseBucket
}
