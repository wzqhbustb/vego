package catalog

import "hash/fnv"

// HashID converts a string ID to an int64 hash for column storage.
func HashID(id string) int64 {
	h := fnv.New64a()
	h.Write([]byte(id))
	return int64(h.Sum64())
}
