package main

import (
	"github.com/kohkimakimoto/bucketstore"
	"fmt"
)

func main() {
	// open database
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

	err = bucket.Delete([]byte("user001"))
	if err != nil {
		panic(err)
	}

	// put data
	err = bucket.PutRaw([]byte("user001"), []byte(`{"name": "hoge", "age": 20}`))
	if err != nil {
		panic(err)
	}
	err = bucket.PutRaw([]byte("user002"), []byte(`{"name": "foo", "age": 31}`))
	if err != nil {
		panic(err)
	}
	err = bucket.PutRaw([]byte("user003"), []byte(`{"name": "bar", "age": 18}`))
	if err != nil {
		panic(err)
	}
	err = bucket.PutRaw([]byte("user004"), []byte(`{"name": "aaa", "age": 40}`))
	if err != nil {
		panic(err)
	}
	err = bucket.PutRaw([]byte("user005"), []byte(`{"name": "xxx", "age": 41}`))
	if err != nil {
		panic(err)
	}
	err = bucket.PutRaw([]byte("user006"), []byte(`{"name": "ccc", "age": 50}`))
	if err != nil {
		panic(err)
	}

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

