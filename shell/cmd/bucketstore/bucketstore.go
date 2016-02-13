package main

import (
	"flag"
	"fmt"
	"github.com/kohkimakimoto/bucketstore"
	"github.com/kohkimakimoto/bucketstore/shell"
	"os"
)

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			os.Exit(1)
		}
	}()

	os.Exit(realMain())
}

func realMain() int {
	var optr, optn bool
	flag.BoolVar(&optr, "r", false, "")
	flag.BoolVar(&optr, "read", false, "")
	flag.BoolVar(&optn, "n", false, "")
	flag.BoolVar(&optn, "new", false, "")
	flag.Usage = printUsage
	flag.Parse()

	if len(flag.Args()) == 0 {
		flag.Usage()
		return 0
	}

	if len(flag.Args()) != 1 {
		fmt.Fprintf(os.Stderr, "Error: illegal argument.\n")
		flag.Usage()
		return 1
	}

	path := flag.Arg(0)

	if optn {
		// only create new database
		if _, err := os.Stat(path); err == nil {
			fmt.Fprintf(os.Stderr, "Error: '%s' is already exists.\n", path)
			return 1
		}

		ds, err := bucketstore.Open(path, 0600, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v", err)
			return 1
		}
		ds.Close()
		return 0
	}

	// setup datastore options
	options := bucketstore.NewOptions()
	if optr {
		options.ReadOnly = true
	}

	// setup shell
	sh := shell.NewShell()
	sh.Path = path
	sh.Options = options

	if err := sh.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	return 0
}

func printUsage() {
	fmt.Println(`Usage: bucketstore [<options>] <database_file>

Options:
  -r|-read      Load a database file by read only mode.
  -n|-new       Create a new database.
`)
}
