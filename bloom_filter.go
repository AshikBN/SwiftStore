package swiftdb

import (
	"github.com/spaolacci/murmur3"
)

func newBloomFilter(size int64) *BloomFilter {
	return &BloomFilter{
		Bitset: make([]bool, size),
		Size:   size,
	}
}

func (bf *BloomFilter) Add(item []byte) {
	//generate hash values

	hash1 := murmur3.Sum64(item)
	hash2 := murmur3.Sum64WithSeed(item, 1)
	hash3 := murmur3.Sum64WithSeed(item, 2)

	bf.Bitset[hash1%uint64(bf.Size)] = true
	bf.Bitset[hash2%uint64(bf.Size)] = true
	bf.Bitset[hash3%uint64(bf.Size)] = true
}

func (bf *BloomFilter) Test(item []byte) bool {
	hash1 := murmur3.Sum64(item)
	hash2 := murmur3.Sum64WithSeed(item, 1)
	hash3 := murmur3.Sum64WithSeed(item, 2)

	return bf.Bitset[hash1%uint64(bf.Size)] &&
		bf.Bitset[hash2%uint64(bf.Size)] &&
		bf.Bitset[hash3%uint64(bf.Size)]
}
