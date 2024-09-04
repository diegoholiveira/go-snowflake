package snowflake

import (
	"hash/fnv"
	"sync"
	"time"
)

type IDGenerator struct {
	datacenterID     uint64
	machineID        uint64
	counter          uint64
	counterTimestamp uint64
	epoch            uint64
	locker           sync.Mutex
}

func NewIDGenerator(datacenterID, machineID string) *IDGenerator {
	epoch := uint64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	return &IDGenerator{
		datacenterID:     hashID(datacenterID),
		machineID:        hashID(machineID),
		counter:          0,
		counterTimestamp: uint64(time.Now().UnixMilli()),
		epoch:            epoch,
	}
}

func (generator *IDGenerator) Next() uint64 {
	generator.locker.Lock()
	defer generator.locker.Unlock()

	now := uint64(time.Now().UnixMilli()) - generator.epoch

	if now == generator.counterTimestamp {
		generator.counter++
		if generator.counter >= 4096 {
			for now <= generator.counterTimestamp {
				now = uint64(time.Now().UnixMilli()) - generator.epoch
				time.Sleep(time.Millisecond)
			}
			generator.counter = 0
		}
	} else {
		generator.counter = 0
		generator.counterTimestamp = now
	}

	// 0 << 63       : Ensures the most significant bit (bit 63) is reserved.
	// elapsed << 22 : Shifts now left by 22 bits so that it occupies bits 22 to 62 (41 bits).
	// hashID << 17  : Shifts hashID(generator.datacenterID) left by 17 bits so that it occupies bits 17 to 21 (5 bits).
	// hashID << 12  : Shifts hashID(generator.machineID) left by 12 bits so that it occupies bits 12 to 16 (5 bits).
	// counter       : Directly occupies bits 0 to 11 (12 bits).
	return (0 << 63) | (now << 22) | (generator.datacenterID << 17) | (generator.machineID << 12) | generator.counter
}

// hashID hashes the input string and returns a 5-bit value (0–31)
func hashID(s string) uint64 {
	// Use FNV-1a hash function for better distribution
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(s))
	hash := hasher.Sum64()

	// Extract the 5 least significant bits to ensure a value between 0 and 31
	return hash & 0x1F
}
