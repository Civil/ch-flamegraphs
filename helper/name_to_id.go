package helper

import (
	"github.com/cespare/xxhash"
)

func NameToIdUint64(name string) uint64 {
	hash := xxhash.New()
	hash.Write([]byte(name))
	return hash.Sum64()
}

func NameToIdInt64(name string) int64 {
	return int64(NameToIdUint64(name))
}
