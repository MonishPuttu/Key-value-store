package engine

import (
	"hash/fnv"
	"math"
)

// bloomFilter is a probabilistic set: MayContain never returns a false
// negative, and returns a false positive with probability ~0.6185^bitsPerKey
// (≈1% at 10 bits per key). Each SSTable stores one, so a point lookup for
// a missing key can skip the table without reading any data block.
type bloomFilter struct {
	bits []byte
	k    uint8
}

func newBloomFilter(keys [][]byte, bitsPerKey int) bloomFilter {
	// Optimal number of hash functions: k = ln2 * m/n.
	k := uint8(math.Max(1, math.Min(30, math.Round(float64(bitsPerKey)*math.Ln2))))
	nbits := max(64, len(keys)*bitsPerKey)
	nbytes := (nbits + 7) / 8
	nbits = nbytes * 8
	f := bloomFilter{bits: make([]byte, nbytes), k: k}
	for _, key := range keys {
		h1, h2 := bloomHash(key)
		for i := uint32(0); i < uint32(k); i++ {
			bit := (h1 + i*h2) % uint32(nbits)
			f.bits[bit/8] |= 1 << (bit % 8)
		}
	}
	return f
}

// bloomHash derives two hashes for double hashing (Kirsch–Mitzenmacher):
// g_i(x) = h1(x) + i*h2(x) behaves like k independent hash functions.
func bloomHash(key []byte) (uint32, uint32) {
	h := fnv.New64a()
	_, _ = h.Write(key)
	s := h.Sum64()
	return uint32(s), uint32(s>>32) | 1
}

func (f bloomFilter) MayContain(key []byte) bool {
	if len(f.bits) == 0 {
		return true
	}
	nbits := uint32(len(f.bits) * 8)
	h1, h2 := bloomHash(key)
	for i := uint32(0); i < uint32(f.k); i++ {
		bit := (h1 + i*h2) % nbits
		if f.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

func (f bloomFilter) encode() []byte { return append(append([]byte{}, f.bits...), f.k) }

func decodeBloom(b []byte) bloomFilter {
	if len(b) < 2 {
		return bloomFilter{}
	}
	return bloomFilter{bits: b[:len(b)-1], k: b[len(b)-1]}
}
