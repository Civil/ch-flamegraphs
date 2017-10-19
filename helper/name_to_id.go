package helper

import (
	"hash/fnv"
)

func NameToIdUint64(name string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(name))
	return hash.Sum64()
}

func NameToIdInt64(name string) int64 {
	return int64(NameToIdUint64(name))
}
