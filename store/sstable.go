package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"

	"github.com/aharshit/Distributed-Key-Value-Cache/utils"
)

const (
	DataFileExtension  string = ".data"
	IndexFileExtension string = ".index"
	BloomFileExtension string = ".bloom"

	SparseIndexSampleSize int = 1000
)

var sstTableCounter uint32

type SSTable struct {
	dataFile    *os.File
	indexFile   *os.File
	bloomFilter *BloomFilter
	sstCounter  uint32
	minKey      string
	maxKey      string
	sizeInBytes uint32
	sparseKeys  []sparseIndex
}

func InitSSTableOnDisk(directory string, entries *[]Record) (*SSTable, error) {
	atomic.AddUint32(&sstTableCounter, 1)
	table := &SSTable{
		sstCounter: sstTableCounter,
	}
	err := table.InitTableFiles(directory)
	if err != nil {
		return nil, err
	}
	err2 := writeEntriesToSST(entries, table)
	if err2 != nil {
		return nil, err
	}

	return table, nil
}

func (sst *SSTable) InitTableFiles(directory string) error {
	if err := os.MkdirAll("../storage", 0755); err != nil {
		return err
	}

	dataFile, err := os.Create(getNextSstFilename(directory, sst.sstCounter) + DataFileExtension)

	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}

	indexFile, err := os.Create(getNextSstFilename(directory, sst.sstCounter) + IndexFileExtension)

	if err != nil {
		err := dataFile.Close()
		if err != nil {
			return err
		}
		return fmt.Errorf("failed to create index file: %w", err)
	}

	bloomFile, err := os.Create(getNextSstFilename(directory, sst.sstCounter) + BloomFileExtension)

	if err != nil {
		err := dataFile.Close()
		if err != nil {
			return err
		}
		err2 := indexFile.Close()
		if err2 != nil {
			return err2
		}
		return fmt.Errorf("failed to create bloom filter file: %w", err)
	}

	sst.dataFile, sst.indexFile = dataFile, indexFile
	sst.bloomFilter = NewBloomFilter(bloomFile)

	return nil
}

func getNextSstFilename(directory string, sstCounter uint32) string {
	return fmt.Sprintf("../%s/sst_%d", directory, sstCounter)
}

type sparseIndex struct {
	keySize    uint32
	key        string
	byteOffset uint32
}

func writeEntriesToSST(sortedEntries *[]Record, table *SSTable) error {
	buf := new(bytes.Buffer)
	var byteOffsetCounter uint32

	table.minKey = (*sortedEntries)[0].Key
	table.maxKey = (*sortedEntries)[len(*sortedEntries)-1].Key

	for i := range *sortedEntries {
		table.sizeInBytes += (*sortedEntries)[i].RecordSize
		if i%SparseIndexSampleSize == 0 {
			table.sparseKeys = append(table.sparseKeys, sparseIndex{
				keySize:    (*sortedEntries)[i].Header.KeySize,
				key:        (*sortedEntries)[i].Key,
				byteOffset: byteOffsetCounter,
			})
		}
		byteOffsetCounter += (*sortedEntries)[i].RecordSize
		err := (*sortedEntries)[i].EncodeKV(buf)
		if err != nil {
			return err
		}
	}

	if err := utils.WriteToFile(buf.Bytes(), table.dataFile); err != nil {
		fmt.Println("write to sst err:", err)
	}
	utils.Logf("SPARSE KEYS: %v", table.sparseKeys)
	err := populateSparseIndexFile(&table.sparseKeys, table.indexFile)
	if err != nil {
		return err
	}

	table.bloomFilter.InitBloomFilterAttrs(uint32(len(*sortedEntries)))
	populateBloomFilter(sortedEntries, table.bloomFilter)

	return nil
}

func populateSparseIndexFile(indices *[]sparseIndex, indexFile *os.File) error {
	buf := new(bytes.Buffer)
	for i := range *indices {
		err := binary.Write(buf, binary.LittleEndian, (*indices)[i].keySize)
		if err != nil {
			return err
		}
		buf.WriteString((*indices)[i].key)
		err2 := binary.Write(buf, binary.LittleEndian, (*indices)[i].byteOffset)
		if err2 != nil {
			return err2
		}
	}

	if err := utils.WriteToFile(buf.Bytes(), indexFile); err != nil {
		fmt.Println("write to indexfile err:", err)
	}
	return nil

}

func populateBloomFilter(entries *[]Record, bloomFilter *BloomFilter) {
	for i := range *entries {
		err := bloomFilter.Add((*entries)[i].Key)
		if err != nil {
			return
		}
	}

	bfBytes := make([]byte, bloomFilter.bitSetSize)
	for i, b := range bloomFilter.bitSet {
		if b {
			bfBytes[i] = 1
		} else {
			bfBytes[i] = 0
		}
	}
	if err := utils.WriteToFile(bfBytes, bloomFilter.file); err != nil {
		fmt.Println("write to bloomfile err:", err)
	}
}

func (sst *SSTable) Get(key string) (string, error) {
	if key < sst.minKey || key > sst.maxKey {
		return "<!>", utils.ErrKeyNotWithinTable
	}

	if !sst.bloomFilter.MightContain(key) {
		utils.LogRED("BLOOM FILTER: %s is not a member of this table", key)
		return "", utils.ErrKeyNotWithinTable
	}

	currOffset := sst.sparseKeys[sst.getCandidateByteOffsetIndex(key)].byteOffset
	if _, err := sst.dataFile.Seek(int64(currOffset), 0); err != nil {
		return "", err
	}
	var keyFound = false
	var eofErr error

	for !keyFound || eofErr == nil {
		currEntry := make([]byte, 17)
		_, err := io.ReadFull(sst.dataFile, currEntry)
		if errors.Is(err, io.EOF) {
			return "", err
		}

		h := &Header{}
		err2 := h.DecodeHeader(currEntry)
		if err2 != nil {
			return "", err2
		}

		currOffset += headerSize
		_, err3 := sst.dataFile.Seek(int64(currOffset), 0)
		if err3 != nil {
			return "", err3
		}
		currRecord := make([]byte, h.KeySize+h.ValueSize)
		if _, err2 := io.ReadFull(sst.dataFile, currRecord); err2 != nil {
			fmt.Println("READFULL ERR:", err2)
			return "", err2
		}
		currEntry = append(currEntry, currRecord...)
		r := &Record{}
		err4 := r.DecodeKV(currEntry)
		if err4 != nil {
			return "", err4
		}

		if r.Key == key {
			utils.LogGREEN("FOUND KEY %s -> VALUE %s\n", key, r.Value)
			return r.Value, nil
		} else if r.Key > key {
			return "", utils.ErrKeyNotWithinTable
		} else {
			currOffset += r.Header.KeySize + r.Header.ValueSize
			_, err2 := sst.dataFile.Seek(int64(currOffset), 0)
			if err2 != nil {
				return "", err2
			}
		}

	}

	return "", utils.ErrKeyNotFound
}

func (sst *SSTable) getCandidateByteOffsetIndex(targetKey string) int {
	low := 0
	high := len(sst.sparseKeys) - 1

	for low <= high {
		mid := (low + high) / 2

		cmp := strings.Compare(targetKey, sst.sparseKeys[mid].key)
		if cmp > 0 {
			low = mid + 1
		} else if cmp < 0 {
			high = mid - 1
		} else {
			return mid
		}
	}
	utils.LogCYAN("CANDIDATE BYTE OFFSET: %d AT INDEX %d", sst.sparseKeys[low-1].byteOffset, uint32(low-1))
	return low - 1
}
