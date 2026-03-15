package store

import (
	"github.com/user/kvcache/utils"
)

type BucketManager struct {
	buckets           map[int]*Bucket
	highestLvl        int
	minTableThreshold int
	maxTableThreshold int
}

func InitBucketManager() *BucketManager {
	manager := &BucketManager{
		buckets:           make(map[int]*Bucket),
		highestLvl:        1,
		minTableThreshold: 4,
		maxTableThreshold: 12,
	}
	manager.buckets[1] = InitEmptyBucket()

	return manager
}

func (bm *BucketManager) InsertTable(table *SSTable) error {
	var levelToAppend = 1

	for currLvl := bm.highestLvl; currLvl > 0; currLvl-- {
		bkt := bm.buckets[currLvl]

		calculatedLevelReturn := calculateLevel(bkt, table)
		levelToAppend = currLvl + calculatedLevelReturn

		if calculatedLevelReturn == -1 {
			continue
		}

		if calculatedLevelReturn == 0 {
			bm.buckets[currLvl].AppendTableToBucket(table)
		} else { // calculatedLevelReturn == 1
			bm.buckets[levelToAppend] = InitEmptyBucket()
			bm.buckets[levelToAppend].AppendTableToBucket(table)
			bm.highestLvl++
		}
		break
	}

	if bm.shouldCompact(levelToAppend) {
		err := bm.compact(levelToAppend)
		if err != nil {
			return err
		}
	}

	bm.DebugBM()
	return nil
}

func (bm *BucketManager) RetrieveKey(key *string) (string, error) {
	for lvl := bm.highestLvl; lvl > 0; lvl-- {
		for _, table := range bm.buckets[lvl].tables {
			val, err := table.Get(*key)
			if err == nil {
				return val, nil
			}
		}
	}
	return "<!not_found>", utils.ErrKeyNotFound
}

func (bm *BucketManager) DebugBM() {
	utils.Log("Length of each bucket:")
	for k, v := range bm.buckets {
		utils.LogCYAN("Level = %d, Len of level = %d", k, len(v.tables))
	}
}

func (bm *BucketManager) compact(level int) error {
	bkt := bm.buckets[level]
	mergedTable, err := bkt.TriggerCompaction()

	if mergedTable != nil {
		err := bm.InsertTable(mergedTable)
		if err != nil {
			return err
		}
	}

	return err
}

func (bm *BucketManager) shouldCompact(level int) bool {
	return bm.buckets[level].NeedsCompaction(bm.minTableThreshold, bm.maxTableThreshold)
}

func calculateLevel(bucket *Bucket, table *SSTable) int {
	lowerSizeThreshold := uint32(bucket.bucketLow * float32(bucket.avgBucketSize))
	higherSizeThreshold := uint32(bucket.bucketHigh * float32(bucket.avgBucketSize))

	if table.sizeInBytes < lowerSizeThreshold {
		return -1
	} else if table.sizeInBytes > higherSizeThreshold {
		return 1
	} else {
		return 0
	}
}
