package store

import (
	"cmp"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/user/kvcache/utils"
)

type Bucket struct {
	minTableSize  uint32
	avgBucketSize uint32
	bucketLow     float32
	bucketHigh    float32
	tables        []SSTable
}

const DefaultTableSizeInBytes uint32 = 3_000

func InitBucket(table *SSTable) *Bucket {
	bucket := &Bucket{
		minTableSize: DefaultTableSizeInBytes,
		bucketLow:    0.5,
		bucketHigh:   1.5,
		tables:       []SSTable{*table},
	}
	bucket.calculateAvgBucketSize()
	return bucket
}

func InitEmptyBucket() *Bucket {
	bucket := &Bucket{
		minTableSize:  DefaultTableSizeInBytes,
		avgBucketSize: DefaultTableSizeInBytes,
		bucketLow:     0.5,
		bucketHigh:    1.5,
		tables:        []SSTable{},
	}
	return bucket
}

func (b *Bucket) AdjustSizeThresholdParams(bucketLow, bucketHigh float32) {
	b.bucketLow = bucketLow
	b.bucketHigh = bucketHigh
}

func (b *Bucket) AppendTableToBucket(table *SSTable) {
	if table.sizeInBytes < b.minTableSize {
		return
	}

	if len(b.tables) == 0 {
		b.tables = append(b.tables, *table)
		b.calculateAvgBucketSize()
		return
	}

	lowerSizeThreshold := uint32(b.bucketLow * float32(b.avgBucketSize))
	higherSizeThreshold := uint32(b.bucketHigh * float32(b.avgBucketSize))

	if lowerSizeThreshold <= table.sizeInBytes && table.sizeInBytes <= higherSizeThreshold {
		b.tables = append(b.tables, *table)
	} else {
		utils.Log("Could not append table. Out of range")
	}

	b.calculateAvgBucketSize()
}

func (b *Bucket) calculateAvgBucketSize() {
	var sum uint32 = 0
	for i := range b.tables {
		sum += b.tables[i].sizeInBytes
	}
	b.avgBucketSize = sum / uint32(len(b.tables))
}

func (b *Bucket) NeedsCompaction(minNumTables, maxNumTables int) bool {
	return len(b.tables) >= minNumTables && len(b.tables) <= maxNumTables
}

func (b *Bucket) TriggerCompaction() (*SSTable, error) {
	utils.LogGREEN("STARTING COMPACTION WITH LENGTH %d", len(b.tables))

	var allSortedRuns [][]Record

	for i := range b.tables {
		var currSortedRun []Record
		var currOffset uint32

		_, err := b.tables[i].dataFile.Seek(int64(currOffset), 0)
		if err != nil {
			return nil, err
		}
		for {
			currEntry := make([]byte, headerSize)
			_, err := io.ReadFull(b.tables[i].dataFile, currEntry)
			if errors.Is(err, io.EOF) {
				break
			}

			h := &Header{}
			err = h.DecodeHeader(currEntry)
			if err != nil {
				return nil, err
			}

			currOffset += headerSize
			_, err = b.tables[i].dataFile.Seek(int64(currOffset), 0)
			if err != nil {
				return nil, err
			}
			currRecord := make([]byte, h.KeySize+h.ValueSize)
			if _, err := io.ReadFull(b.tables[i].dataFile, currRecord); err != nil {
				fmt.Println("READFULL ERR:", err)
				break
			}
			currEntry = append(currEntry, currRecord...)
			r := &Record{}
			err = r.DecodeKV(currEntry)
			if err != nil {
				return nil, err
			}

			currSortedRun = append(currSortedRun, *r)

			currOffset += r.Header.KeySize + r.Header.ValueSize
			_, err = b.tables[i].dataFile.Seek(int64(currOffset), 0)
			if err != nil {
				return nil, err
			}
		}
		allSortedRuns = append(allSortedRuns, currSortedRun)
	}

	h := MinRecordHeap{}
	for i := range allSortedRuns {
		for j := range allSortedRuns[i] {
			heap.Push(&h, allSortedRuns[i][j])
		}
	}

	utils.LogGREEN("Heap len = %d", h.Len())
	finalSortedRun := make([]Record, 0)
	for h.Len() > 0 {
		ele := heap.Pop(&h)
		finalSortedRun = append(finalSortedRun, ele.(Record))
	}

	filterAndDeleteTombstones(&finalSortedRun)
	removeOutdatedEntires(&finalSortedRun)

	mergedSSTable, err := InitSSTableOnDisk("storage", &finalSortedRun)
	if err != nil {
		return nil, err
	}

	err = deleteOldSSTables(&b.tables)
	if err != nil {
		return nil, err
	}

	return mergedSSTable, nil
}

func filterAndDeleteTombstones(sortedRun *[]Record) {
	var collectedTombstones []string

	for i := range *sortedRun {
		if (*sortedRun)[i].Header.Tombstone == 1 {
			collectedTombstones = append(collectedTombstones, (*sortedRun)[i].Key)
		}
	}

	for i := 0; i < len(*sortedRun); {
		if slices.Contains(collectedTombstones, (*sortedRun)[i].Key) {
			if i < len(*sortedRun)-1 {
				*sortedRun = slices.Delete(*sortedRun, i, i+1)
			} else {
				*sortedRun = (*sortedRun)[:len(*sortedRun)-1]
			}
		} else {
			i++
		}
	}
}

func removeOutdatedEntires(sortedRun *[]Record) {
	var tempMap = make(map[string][]Record)

	for i := range *sortedRun {
		tempMap[(*sortedRun)[i].Key] = append(tempMap[(*sortedRun)[i].Key], (*sortedRun)[i])
	}

	for _, v := range tempMap {
		if len(v) > 1 {
			slices.SortFunc(v, func(a, b Record) int {
				return cmp.Compare(a.Header.TimeStamp, b.Header.TimeStamp)
			})

			for i := 0; i < len(v)-1; i++ {
				idx := slices.Index(*sortedRun, v[i])
				*sortedRun = slices.Delete(*sortedRun, idx, idx+1)
			}
		}
	}
}

func deleteOldSSTables(tables *[]SSTable) error {
	for i := range *tables {
		files := []string{(*tables)[i].dataFile.Name(), (*tables)[i].indexFile.Name(), (*tables)[i].bloomFilter.file.Name()}

		for _, file := range files {
			if err := os.Remove(file); err != nil {
				utils.Log("DELETION ERROR")
				return err
			}
		}
	}
	*tables = []SSTable{} // empty the slice
	return nil
}
