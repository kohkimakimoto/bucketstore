package main

import (
	"github.com/kohkimakimoto/bucketstore"
	"log"
)

func main() {
	db, err := bucketstore.Open("todo.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}

