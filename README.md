# Bucketstore

Bucketstore is a pure Go embedded database engine to store JSON structure data.
It is based on [Bolt](https://github.com/boltdb/bolt).

**This software is in the development stage. The code and api may be changed drastically.**

## Installation

Run `go get`.

```sh
$ go get github.com/kohkimakimoto/bucketstore
```

## Usage

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

* **Database**: The top-level object stores all buckets.
* **Bucket**: Collection of items.
* **Item**: key/value pairs. value is a JSON formatted data.

## Author

Kohki Makimoto <kohki.makimoto@gmail.com>

## License

The MIT License (MIT)

## Todo

* documentation
* tests
* demo app
