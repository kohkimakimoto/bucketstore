package bucketstore

type Query struct {
	bucket *Bucket
	Offset uint64
	Limit  uint64
	Filter Filter
}

func newQuery(bucket *Bucket) *Query {
	return &Query{
		bucket: bucket,
		Filter: &OrderByFilter{},
	}
}

func (q *Query) AsList() (items []*Item, err error) {
	return q.getList()
}

func (q *Query) AsSingle() (item *Item, err error) {
	items, err := q.getList()
	if err != nil {
		return nil, err
	}

	if items == nil {
		return nil, nil
	}

	return items[0], nil
}

func (q *Query) getList() (items []*Item, err error) {
	if q.bucket.baseBucket != nil {
		basebucket := q.bucket.baseBucket

		items = q.Filter.getItems(q, basebucket)
		return items, nil
	}

	err = q.bucket.datastore.View(func(tx *Tx) error {
		basebucket, err := tx.baseBucket([]byte(q.bucket.name))
		if err != nil {
			return err
		}

		if basebucket == nil {
			return nil
		}

		items = q.Filter.getItems(q, basebucket)

		return nil
	})

	return items, err
}
