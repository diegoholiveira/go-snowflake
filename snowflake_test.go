package snowflake_test

import (
	"fmt"
	"math/bits"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/diegoholiveira/go-snowflake"
)

func TestSnowflakeIDGenerator_naive_test(t *testing.T) {
	generator := snowflake.NewIDGenerator("dc1", "server1")

	id := generator.Next()

	assert.Greater(t, id, uint64(0), "id > 0")
	// Ensure 63 bits because one bit is a sign bit (reserved)
	assert.GreaterOrEqual(t, 63, bits.Len64(id), "ensure 64 bits")
}

func TestSnowflakeIDGenerator_ensure_unique(t *testing.T) {
	generator1 := snowflake.NewIDGenerator("dc1", "server1")
	generator2 := snowflake.NewIDGenerator("dc2", "server2")

	uniques := make(map[uint64]int)
	for i := 0; 100_000 > i; i++ {
		id1 := generator1.Next()
		id2 := generator2.Next()

		if _, found := uniques[id1]; !found {
			uniques[id1] = 0
		}

		if _, found := uniques[id2]; !found {
			uniques[id2] = 0
		}

		uniques[id1] += 1
		uniques[id2] += 1
	}

	notUniques := 0
	for _, count := range uniques {
		if count > 1 {
			notUniques += 1
		}
	}

	assert.True(t, notUniques == 0, fmt.Sprintf("not unique ids: `%d`", notUniques))
}

func TestSnowflakeIDGenerator_ensure_sortable(t *testing.T) {
	generator1 := snowflake.NewIDGenerator("dc1", "server1")
	generator2 := snowflake.NewIDGenerator("dc1", "server2")

	original := make([]uint64, 0)

	for i := 0; 100 > i; i++ {
		original = append(original, generator1.Next(), generator2.Next())
	}

	// Ensure original is sorted because it may not be due to concurrency
	sort.Slice(original, func(i, j int) bool {
		return original[j] > original[i]
	})

	modified := make([]uint64, len(original))

	copy(modified, original)

	assert.Equal(t, original, modified, "original should be equal the modified just after copying it")

	rand.Shuffle(len(modified), func(i, j int) {
		modified[i], modified[j] = modified[j], modified[i]
	})

	assert.NotEqual(t, original, modified, "original and modified should not be equal after shuffle the modified")

	sort.Slice(modified, func(i, j int) bool {
		return modified[j] > modified[i]
	})

	assert.Equal(t, original, modified, "original and modified must be equal now that it's sorted")
}
