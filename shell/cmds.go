package shell

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/kohkimakimoto/bucketstore"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

type CmdFunc func(sh *Shell, args []*Token) (*Response, error)

var Cmds = map[string]CmdFunc{
	"exit":    doExit,
	"help":    doHelp,
	"buckets": doBuckets,
	"put":     doPut,
	"post":    doPost,
	"get":     doGet,
	"delete":  doDelete,
	"select":  doSelect,
}

func doExit(sh *Shell, args []*Token) (*Response, error) {
	sh.Exit()
	return nil, nil
}

func doHelp(sh *Shell, args []*Token) (*Response, error) {
	pager := os.Getenv("PAGER")
	if pager != "" {
		var cmd *exec.Cmd
		if runtime.GOOS == "windows" {
			cmd = exec.Command(pager)
		} else {
			cmd = exec.Command(pager)
		}

		cmd.Stdout = sh.Stdout
		cmd.Stderr = sh.Stderr
		cmd.Stdin = strings.NewReader(helpText)

		err := cmd.Run()
		if err != nil {
			return nil, err
		}

		return nil, nil
	}

	fmt.Fprintf(sh.Stdout, helpText)
	return nil, nil
}

func doBuckets(sh *Shell, args []*Token) (*Response, error) {
	for _, token := range args {
		if token.DataType == DataTypeTerm && strings.HasPrefix(token.Buf, "-") {
			switch {
			default:
				return nil, fmt.Errorf("unsupported option: %s", token.Buf)
			}
		}
	}

	var res *Response
	err := sh.DB.View(func(tx *bucketstore.Tx) error {
		buckets := []string{}

		err := tx.BucketNames(func(name string) error {
			buckets = append(buckets, name)
			return nil
		})

		if err != nil {
			return err
		}

		res = &Response{
			Status: "ok",
			Count:  uint64(len(buckets)),
			Body:   buckets,
		}

		return nil
	})

	return res, err
}

func doPut(sh *Shell, args []*Token) (*Response, error) {
	for _, token := range args {
		if token.DataType == DataTypeTerm && strings.HasPrefix(token.Buf, "-") {
			switch {
			default:
				return nil, fmt.Errorf("unsupported option: %s", token.Buf)
			}
		}
	}

	if len(args) != 3 {
		return nil, fmt.Errorf("invalid arguments. 'put' requires 3 arguments")
	}

	if args[0].DataType != DataTypeString {
		return nil, fmt.Errorf("the bucket name must be string: %s", args[0].Buf)
	}

	bucketName := args[0].Buf
	keyName := args[1].ToMustBytes()
	value := args[2].ToMustBytes()

	bucket := sh.DB.Bucket(bucketName)
	err := bucket.PutRaw(keyName, value)
	if err != nil {
		return nil, err
	}

	responseItem := map[string]interface{}{}

	responseItem["key"] = "0x" + hex.EncodeToString(keyName)

	jsonValue := map[string]interface{}{}
	err = json.Unmarshal(value, &jsonValue)
	if err == nil {
		responseItem["value"] = jsonValue
	} else {
		responseItem["value"] = value
	}

	return &Response{
		Status: "ok",
		Bucket: bucketName,
		Body:   responseItem,
	}, nil
}

func doPost(sh *Shell, args []*Token) (*Response, error) {
	for _, token := range args {
		if token.DataType == DataTypeTerm && strings.HasPrefix(token.Buf, "-") {
			switch {
			default:
				return nil, fmt.Errorf("unsupported option: %s", token.Buf)
			}
		}
	}

	if len(args) != 2 {
		return nil, fmt.Errorf("invalid arguments. 'post' requires 2 arguments")
	}

	if args[0].DataType != DataTypeString {
		return nil, fmt.Errorf("the bucket name must be string: %s", args[0].Buf)
	}

	bucketName := args[0].Buf
	value := args[1].ToMustBytes()

	bucket := sh.DB.Bucket(bucketName)
	key, err := bucket.NextSequence()
	if err != nil {
		return nil, err
	}
	keyName := bucketstore.Uint64ToBytes(key)
	err = bucket.PutRaw(keyName, value)
	if err != nil {
		return nil, err
	}

	responseItem := map[string]interface{}{}

	responseItem["key"] = "0x" + hex.EncodeToString(keyName)

	jsonValue := map[string]interface{}{}
	err = json.Unmarshal(value, &jsonValue)
	if err == nil {
		responseItem["value"] = jsonValue
	} else {
		responseItem["value"] = value
	}

	return &Response{
		Status: "ok",
		Bucket: bucketName,
		Body:   responseItem,
	}, nil

	return nil, nil
}


func doGet(sh *Shell, args []*Token) (*Response, error) {
	for _, token := range args {
		if token.DataType == DataTypeTerm && strings.HasPrefix(token.Buf, "-") {
			switch {
			default:
				return nil, fmt.Errorf("unsupported option: %s", token.Buf)
			}
		}
	}

	if len(args) != 2 {
		return nil, fmt.Errorf("invalid arguments. 'get' requires 2 arguments")
	}

	if args[0].DataType != DataTypeString {
		return nil, fmt.Errorf("the bucket name must be string: %s", args[0].Buf)
	}

	bucketName := args[0].Buf

	keyName := args[1].ToMustBytes()
	bucket := sh.DB.Bucket(bucketName)

	value, err := bucket.GetRaw(keyName)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return &Response{
			Status: "ok",
			Body:   nil,
		}, nil
	}

	responseItem := map[string]interface{}{}

	responseItem["key"] = "0x" + hex.EncodeToString(keyName)

	jsonValue := map[string]interface{}{}
	err = json.Unmarshal(value, &jsonValue)
	if err == nil {
		responseItem["value"] = jsonValue
	} else {
		responseItem["value"] = value
	}

	return &Response{
		Status: "ok",
		Bucket: bucketName,
		Body:   responseItem,
	}, nil
}

func doDelete(sh *Shell, args []*Token) (*Response, error) {
	for _, token := range args {
		if token.DataType == DataTypeTerm && strings.HasPrefix(token.Buf, "-") {
			switch {
			default:
				return nil, fmt.Errorf("unsupported option: %s", token.Buf)
			}
		}
	}

	if len(args) != 2 {
		return nil, fmt.Errorf("invalid arguments. 'delete' requires 2 arguments")
	}

	if args[0].DataType == DataTypeTerm && args[0].Buf == "bucket" {
		// delete bucket
		if args[1].DataType != DataTypeString {
			return nil, fmt.Errorf("invalid arguments")
		}

		bucketName := args[1].Buf
		err := sh.DB.DeleteBucket(bucketName)
		if err != nil {
			return nil, err
		}

		return &Response{
			Status: "ok",
		}, nil
	} else {
		// delete item
		if args[0].DataType != DataTypeString {
			return nil, fmt.Errorf("invalid arguments")
		}
		bucketName := args[0].Buf
		keyName := args[1].ToMustBytes()

		bucket := sh.DB.Bucket(bucketName)
		err := bucket.Delete(keyName)
		if err != nil {
			return nil, err
		}

		return &Response{
			Status: "ok",
		}, nil
	}
}

func doSelect(sh *Shell, args []*Token) (*Response, error) {
	// parse options
	var limit uint64
	var offset uint64
	var filter string
	var prefix *Token
	var match *Token
	var min *Token
	var max *Token
	var order bucketstore.OrderBy = bucketstore.OrderByAsc
	var prop string

	var removedIndexes = []int{}
	for i, token := range args {
		if token.DataType == DataTypeTerm && strings.HasPrefix(token.Buf, "-") {
			switch {
			case strings.HasPrefix(token.Buf, "--limit"):
				ui, err := strconv.ParseUint(args[i + 1].Buf, 0, 64)
				if err != nil {
					return nil, err
				}

				limit = ui
				removedIndexes = append(removedIndexes, i)
			case strings.HasPrefix(token.Buf, "--offset"):
				ui, err := strconv.ParseUint(args[i + 1].Buf, 0, 64)
				if err != nil {
					return nil, err
				}

				offset = ui
				removedIndexes = append(removedIndexes, i)
			case strings.HasPrefix(token.Buf, "--filter"):
				filter = args[i + 1].Buf
				if filter == "" {
					return nil, fmt.Errorf("requires filter value")
				}
				removedIndexes = append(removedIndexes, i)
			case strings.HasPrefix(token.Buf, "--prefix"):
				prefix = args[i + 1]
				if prefix == nil {
					return nil, fmt.Errorf("requires prefix value")
				}

				removedIndexes = append(removedIndexes, i)
			case strings.HasPrefix(token.Buf, "--match"):
				match = args[i + 1]
				if match == nil {
					return nil, fmt.Errorf("requires match value")
				}

				removedIndexes = append(removedIndexes, i)
			case strings.HasPrefix(token.Buf, "--min"):
				min = args[i + 1]

				if min == nil {
					return nil, fmt.Errorf("requires min value")
				}

				removedIndexes = append(removedIndexes, i)
			case strings.HasPrefix(token.Buf, "--max"):
				max = args[i + 1]

				if max == nil {
					return nil, fmt.Errorf("requires max value")
				}

				removedIndexes = append(removedIndexes, i)
			case strings.HasPrefix(token.Buf, "--orderby"):
				o := args[i + 1].Buf

				if o == "" {
					return nil, fmt.Errorf("requires orderby value")
				}

				if o == "desc" {
					order = bucketstore.OrderByDesc
				} else if o == "asc" {
					order = bucketstore.OrderByAsc
				} else {
					return nil, fmt.Errorf("invalid order value")
				}

				removedIndexes = append(removedIndexes, i)
			case strings.HasPrefix(token.Buf, "--prop"):
				prop = args[i + 1].Buf

				if prop == "" {
					return nil, fmt.Errorf("requires prop value")
				}

				removedIndexes = append(removedIndexes, i)
			default:
				return nil, fmt.Errorf("invalid option %s", token.Buf)
			}
		}
	}

	// remove options from args
	for i, idx := range removedIndexes {
		removedI := idx - (i * 2)
		args = append(args[:removedI], args[removedI+2:]...)
	}

	if len(args) < 1 {
		return nil, fmt.Errorf("invalid arguments")
	}

	if args[0].DataType != DataTypeString {
		return nil, fmt.Errorf("invalid arguments")
	}
	bucketName := args[0].Buf

	bucket := sh.DB.Bucket(bucketName)
	q := bucket.Query()

	if limit != 0 {
		q.Limit = limit
	}

	if offset != 0 {
		q.Offset = offset
	}

	if filter != "" {
		if filter == "keyPrefix" {
			if prefix == nil {
				return nil, fmt.Errorf("keyPrefix filter requires prefix")
			}

			q.Filter = &bucketstore.KeyPrefixFilter{
				Prefix: prefix.ToMustBytes(),
				OrderBy: order,
			}
		} else if filter == "keyRange" {
			if min == nil || max == nil {
				return nil, fmt.Errorf("keyRange filter requires min and max")
			}

			q.Filter = &bucketstore.KeyRangeFilter{
				Min:     min.ToMustBytes(),
				Max:     max.ToMustBytes(),
				OrderBy: order,
			}
		} else if filter == "propValueMatch" {
			if prop == "" {
				return nil, fmt.Errorf("propValuePrefix filter requires prop")
			}
			if match == nil {
				return nil, fmt.Errorf("propValuePrefix filter requires match")
			}

			q.Filter = &bucketstore.PropValueMatchFilter{
				Property: prop,
				Match:   match.ToMustValue(),
				OrderBy: order,
			}

		} else if filter == "propValuePrefix" {
			if prop == "" {
				return nil, fmt.Errorf("propValuePrefix filter requires prop")
			}
			if prefix == nil {
				return nil, fmt.Errorf("propValuePrefix filter requires prefix")
			}

			q.Filter = &bucketstore.PropValuePrefixFilter{
				Property: prop,
				Prefix:   prefix.ToMustValue(),
				OrderBy: order,
			}

		} else if filter == "propValueRange" {
			if prop == "" {
				return nil, fmt.Errorf("propValueRange filter requires prop")
			}
			if min == nil || max == nil {
				return nil, fmt.Errorf("propValueRange filter requires min and max")
			}

			q.Filter = &bucketstore.PropValueRangeFilter{
				Property: prop,
				Min:     min.ToMustValue(),
				Max:     max.ToMustValue(),
				OrderBy: order,
			}
		} else {
			return nil, fmt.Errorf("invalid filter")
		}
	} else {
		q.Filter = &bucketstore.OrderByFilter{
			OrderBy: order,
		}
	}

	items, err := q.AsList()
	if err != nil {
		return nil, err
	}

	responseBody := []map[string]interface{}{}

	for _, item := range items {
		responseItem := map[string]interface{}{}

		responseItem["key"] = "0x" + hex.EncodeToString(item.Key)
		jsonValue := map[string]interface{}{}
		err = json.Unmarshal(item.Value, &jsonValue)
		if err == nil {
			responseItem["value"] = jsonValue
		} else {
			responseItem["value"] = item.Value
		}

		responseBody = append(responseBody, responseItem)
	}

	res := &Response{
		Status: "ok",
		Bucket: bucketName,
		Count:  uint64(len(items)),
		Body:   responseBody,
	}

	return res, nil
}
