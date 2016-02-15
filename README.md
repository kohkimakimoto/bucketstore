# Bucketstore

Bucketstore is a pure Go embedded database engine to store JSON structure data.
It is based on [Bolt](https://github.com/boltdb/bolt).

**This software is in the development stage. The code and api may be changed drastically.**

Table of contents

* [Installation](#installation)
* [Usage](#usage)
  * [Data Model](#data-model)
  * [Select items by using a query](#select-items-by-using-a-query)
* [Author](#author)
* [License](#license)

## Installation

Run `go get`.

```sh
$ go get github.com/kohkimakimoto/bucketstore
```

## Usage

The following code is an example to operate Bucketstore as a simple key/value store.

```go
package main

import (
	"github.com/kohkimakimoto/bucketstore"
	"fmt"
)

func main() {
	db, err := bucketstore.Open("my.db", 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	bucket := db.Bucket("MyBucket")

	// put key/value item
	err = bucket.PutRaw([]byte("user001"), []byte(`{"name": "kohkimakimoto", "age": 36}`))
	if err != nil {
		panic(err)
	}

	// get value
	v, err := bucket.GetRaw([]byte("user001"))
	if err != nil {
		panic(err)
	}

	fmt.Println(string(v))
	// {"age":36,"name":"kohkimakimoto"}
}
```

### Data Model

Bucketstore has 3 data models and structures flexible datastore.

* **Database**: Database is the top-level object stores all buckets.
* **Bucket**: Bucket is a collection of items.
* **Item**: Items is a key/value pair. The value is a JSON structure data.


### Select items by using a query

```go
func main() {
	// open database
	db, err := bucketstore.Open("my.db", 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	bucket := db.Bucket("MyBucket")

	// put data (ignore errors)
	bucket.PutRaw([]byte("user001"), []byte(`{"name": "hoge", "age": 20}`))
	bucket.PutRaw([]byte("user002"), []byte(`{"name": "foo", "age": 31}`))
	bucket.PutRaw([]byte("user003"), []byte(`{"name": "bar", "age": 18}`))
	bucket.PutRaw([]byte("user004"), []byte(`{"name": "aaa", "age": 40}`))
	bucket.PutRaw([]byte("user005"), []byte(`{"name": "xxx", "age": 41}`))
	bucket.PutRaw([]byte("user006"), []byte(`{"name": "ccc", "age": 50}`))

    // query
	q := bucket.Query()
	q.Filter = &bucketstore.PropValueRangeFilter{
		Property: "age",
		Min: 20,
		Max: 40,
	}
	items, err := q.AsList()
	if err != nil {
		panic(err)
	}

	for _, item := range items {
		fmt.Println(string(item.Key), string(item.Value))
	}
	// user001 {"age":20,"name":"hoge"}
	// user002 {"age":31,"name":"foo"}
	// user004 {"age":40,"name":"aaa"}
}
```

## Author

Kohki Makimoto <kohki.makimoto@gmail.com>

## License

The MIT License (MIT)

## Todo

* documentation
* tests
