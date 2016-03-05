package bucketstore

import (
	"github.com/kohkimakimoto/bucketstore/v/bolt"
)

type Options struct {
	*bolt.Options
}

func NewOptions() *Options {
	opt := &Options{}
	opt.Options = bolt.DefaultOptions

	return opt
}
