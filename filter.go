package bucketstore

import (
	"bytes"
)

type Filter interface {
	getItems(query *Query, bucket *BaseBucket) []*Item
}

type OrderBy int

const (
	OrderByAsc  OrderBy = 0
	OrderByDesc OrderBy = 1
)

type OrderByFilter struct {
	OrderBy OrderBy
}

func (filter *OrderByFilter) getItems(query *Query, bucket *BaseBucket) (items []*Item) {
	c := bucket.Cursor()

	var order = filter.OrderBy

	var offset = query.Offset
	var limit = uint64(0)

	if query.Limit != 0 {
		limit = offset + query.Limit
	}

	var counter uint64 = 0
	if order == OrderByDesc {
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			if offset <= counter {
				items = append(items, &Item{Key: k, Value: v})
			}

			counter++

			if limit != 0 {
				if limit <= counter {
					return items
				}
			}
		}
	} else {
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if offset <= counter {
				items = append(items, &Item{Key: k, Value: v})
			}

			counter++

			if limit != 0 {
				if limit <= counter {
					return items
				}
			}
		}
	}

	return items
}

type KeyPrefixFilter struct {
	Prefix []byte
	OrderBy OrderBy
}

func (filter *KeyPrefixFilter) getItems(query *Query, bucket *BaseBucket) (items []*Item) {
	c := bucket.Cursor()

	var prefix = filter.Prefix
	var order = filter.OrderBy

	var offset = query.Offset
	var limit = uint64(0)

	if query.Limit != 0 {
		limit = offset + query.Limit
	}

	var counter uint64 = 0
	if order == OrderByDesc {
		beginK, beginV := c.Seek(append(prefix, 0xFF))
		if beginK == nil {
			beginK, beginV = c.Last()
		}

		for k, v := beginK, beginV; k != nil && bytes.HasPrefix(k, prefix); k, v = c.Prev() {
			if offset <= counter {
				items = append(items, &Item{Key: k, Value: v})
			}

			counter++

			if limit != 0 {
				if limit <= counter {
					return items
				}
			}
		}

	} else {
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if offset <= counter {
				items = append(items, &Item{Key: k, Value: v})
			}

			counter++

			if limit != 0 {
				if limit <= counter {
					return items
				}
			}
		}

	}

	return items
}

type KeyRangeFilter struct {
	Min     []byte
	Max     []byte
	OrderBy OrderBy
}

func (filter *KeyRangeFilter) getItems(query *Query, bucket *BaseBucket) (items []*Item) {
	c := bucket.Cursor()

	var min = filter.Min
	var max = filter.Max
	var order = filter.OrderBy

	var offset = query.Offset
	var limit = uint64(0)

	if query.Limit != 0 {
		limit = offset + query.Limit
	}

	var counter uint64 = 0
	if order == OrderByDesc {
		beginK, beginV := c.Seek(max)
		if beginK == nil {
			beginK, beginV = c.Last()
		}
		for k, v := beginK, beginV; k != nil && bytes.Compare(k, min) >= 0; k, v = c.Prev() {
			// seek may get a next value that is bigger than maxBytes so needs to check the value.
			if bytes.Compare(k, max) <= 0 {
				if offset <= counter {
					items = append(items, &Item{Key: k, Value: v})
				}

				counter++

				if limit != 0 {
					if limit <= counter {
						return items
					}
				}
			}
		}
	} else {
		for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
			if offset <= counter {
				items = append(items, &Item{Key: k, Value: v})
			}

			counter++

			if limit != 0 {
				if limit <= counter {
					return items
				}
			}
		}
	}

	return items
}

type PropValueMatchFilter struct {
	Property string
	Match   interface{}
	OrderBy  OrderBy
}

func (filter *PropValueMatchFilter) getItems(query *Query, bucket *BaseBucket) (items []*Item) {
	ic := bucket.IndexCursor(filter.Property)

	var match = filter.Match
	var order = filter.OrderBy

	var offset = query.Offset
	var limit = uint64(0)

	if query.Limit != 0 {
		limit = offset + query.Limit
	}

	var counter uint64 = 0

	matchBytes, valueType := toIndexedBytes(match)
	if valueType == valueTypeNoIndex {
		// does not index.
		return nil
	}


	if order == OrderByDesc {
		for idx := ic.SeekLast(valueType, matchBytes); idx != nil && idx.ValueType() == valueType && bytes.Equal(idx.MustValueBytes(), matchBytes); idx = ic.Prev() {
			if offset <= counter {
				k, v := idx.Data()
				items = append(items, &Item{Key: k, Value: v})
			}

			counter++

			if limit != 0 {
				if limit <= counter {
					return items
				}
			}
		}
	} else {
		for idx := ic.SeekFirst(valueType, matchBytes); idx != nil && idx.ValueType() == valueType && bytes.Equal(idx.MustValueBytes(), matchBytes); idx = ic.Next() {
			if offset <= counter {
				k, v := idx.Data()
				items = append(items, &Item{Key: k, Value: v})
			}

			counter++

			if limit != 0 {
				if limit <= counter {
					return items
				}
			}
		}
	}

	return items
}

type PropValuePrefixFilter struct {
	Property string
	Prefix   interface{}
	OrderBy  OrderBy
}

func (filter *PropValuePrefixFilter) getItems(query *Query, bucket *BaseBucket) (items []*Item) {
	ic := bucket.IndexCursor(filter.Property)

	var prefix = filter.Prefix
	var order = filter.OrderBy

	var offset = query.Offset
	var limit = uint64(0)

	if query.Limit != 0 {
		limit = offset + query.Limit
	}

	var counter uint64 = 0

	prefixBytes, valueType := toIndexedBytes(prefix)
	if valueType == valueTypeNoIndex {
		// does not index.
		return nil
	}


	if order == OrderByDesc {
		for idx := ic.SeekLast(valueType, prefixBytes); idx != nil && idx.ValueType() == valueType && bytes.HasPrefix(idx.MustValueBytes(), prefixBytes); idx = ic.Prev() {
			if offset <= counter {
				k, v := idx.Data()
				items = append(items, &Item{Key: k, Value: v})
			}

			counter++

			if limit != 0 {
				if limit <= counter {
					return items
				}
			}
		}
	} else {
		for idx := ic.SeekFirst(valueType, prefixBytes); idx != nil && idx.ValueType() == valueType && bytes.HasPrefix(idx.MustValueBytes(), prefixBytes); idx = ic.Next() {
			if offset <= counter {
				k, v := idx.Data()
				items = append(items, &Item{Key: k, Value: v})
			}

			counter++

			if limit != 0 {
				if limit <= counter {
					return items
				}
			}
		}
	}

	return items
}

type PropValueRangeFilter struct {
	Property string
	Min      interface{}
	Max      interface{}
	OrderBy  OrderBy
}

func (filter *PropValueRangeFilter) getItems(query *Query, bucket *BaseBucket) (items []*Item) {
	ic := bucket.IndexCursor(filter.Property)

	var min = filter.Min
	var max = filter.Max
	var order = filter.OrderBy

	var offset = query.Offset
	var limit = uint64(0)

	if query.Limit != 0 {
		limit = offset + query.Limit
	}

	var counter uint64 = 0

	minBytes, valueType := toIndexedBytes(min)
	maxBytes, valueType2 := toIndexedBytes(max)

	if valueType == valueTypeNoIndex {
		// does not index.
		return nil
	}

	if valueType != valueType2 {
		// unsupport difference value type range
		return nil
	}

	if order == OrderByDesc {
		for idx := ic.SeekLast(valueType, maxBytes); idx != nil && idx.ValueType() == valueType && bytes.Compare(idx.MustValueBytes(), minBytes) >= 0; idx = ic.Prev() {
			if offset <= counter {
				k, v := idx.Data()
				items = append(items, &Item{Key: k, Value: v})
			}

			counter++

			if limit != 0 {
				if limit <= counter {
					return items
				}
			}
		}
	} else {
		for idx := ic.SeekFirst(valueType, minBytes); idx != nil && idx.ValueType() == valueType && bytes.Compare(idx.MustValueBytes(), maxBytes) <= 0; idx = ic.Next() {
			if offset <= counter {
				k, v := idx.Data()
				items = append(items, &Item{Key: k, Value: v})
			}

			counter++

			if limit != 0 {
				if limit <= counter {
					return items
				}
			}
		}
	}

	return items
}
