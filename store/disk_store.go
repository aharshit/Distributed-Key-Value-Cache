package store

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/user/kvcache/utils"

	"github.com/user/kvcache/proto"
)

type DiskStore struct {
	mu                 sync.Mutex
	memtable           *Memtable
	writeAheadLog      *writeAheadLog
	bucketManager      *BucketManager
	immutableMemtables []Memtable
}

type Operation int

const (
	PUT Operation = iota
	GET
	DELETE
)

const FlushSizeThreshold = 1024 * 1024 * 256

func NewCluster(numOfNodes uint32) *Cluster {
	cluster := Cluster{}
	cluster.initNodes(numOfNodes)

	return &cluster
}

func newStore(nodeNum uint32) (*DiskStore, error) {
	ds := &DiskStore{memtable: NewMemtable(), bucketManager: InitBucketManager()}

	logFile, err := os.OpenFile(fmt.Sprintf("../log/kvcache_wal-%d.log", nodeNum), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ds.writeAheadLog = &writeAheadLog{file: logFile}

	return ds, err
}

func (ds *DiskStore) Put(key *string, value *string) error {
	if ds == nil {
		return fmt.Errorf("disk store is not initialized")
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.memtable == nil {
		return fmt.Errorf("memtable is not initialized")
	}

	err := utils.ValidateKV(key, value)
	if err != nil {
		return err
	}

	header := Header{
		CheckSum:  0,
		Tombstone: 0,
		TimeStamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(*key)),
		ValueSize: uint32(len(*value)),
	}
	record := &Record{
		Header:     header,
		Key:        *key,
		Value:      *value,
		RecordSize: headerSize + header.KeySize + header.ValueSize,
	}
	record.Header.CheckSum, err = record.CalculateChecksum()
	if err != nil {
		return err
	}

	ds.memtable.Put(key, record)
	err = ds.writeAheadLog.appendWALOperation(PUT, record)
	if err != nil {
		return err
	}

	if ds.memtable.sizeInBytes >= FlushSizeThreshold {
		ds.immutableMemtables = append(ds.immutableMemtables, *deepCopyMemtable(ds.memtable))
		ds.memtable.clear()
		ds.FlushMemtable()
	}

	return nil
}

func (ds *DiskStore) PutRecordFromGRPC(record *proto.Record) {
	rec := convertProtoRecordToStoreRecord(record)
	ds.memtable.Put(&record.Key, rec)
	fmt.Printf("stored proto record with key = %s into memtable", rec.Key)
}

func (ds *DiskStore) Get(key string) (string, error) {
	if ds == nil {
		return "<!>", fmt.Errorf("disk store is not initialized")
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()

	err := ds.writeAheadLog.appendWALOperation(GET, &Record{Key: key})
	if err != nil {
		return "", err
	}

	record, err := ds.memtable.Get(&key)
	if err == nil {
		return record.Value, nil
	} else if !errors.Is(err, utils.ErrKeyNotFound) {
		return "<!>", err
	}

	return ds.bucketManager.RetrieveKey(&key)
}

func (ds *DiskStore) Delete(key string) error {
	if ds == nil {
		return fmt.Errorf("disk store is not initialized")
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()

	value := ""
	header := Header{
		TimeStamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
	}
	header.MarkTombstone()

	deletionRecord := Record{
		Header:     header,
		Key:        key,
		Value:      value,
		RecordSize: headerSize + header.KeySize + header.ValueSize,
	}
	_, err := deletionRecord.CalculateChecksum()
	if err != nil {
		return err
	}

	ds.memtable.Put(&key, &deletionRecord)
	err = ds.writeAheadLog.appendWALOperation(DELETE, &deletionRecord)
	if err != nil {
		return err
	}

	return nil
}

func (ds *DiskStore) LengthOfMemtable() {
	fmt.Println(len(ds.memtable.data.Keys()))
}

func (ds *DiskStore) FlushMemtable() {
	for i := range ds.immutableMemtables {
		sstable := ds.immutableMemtables[i].Flush("storage")
		err := ds.bucketManager.InsertTable(sstable)
		if err != nil {
			return
		}
		ds.immutableMemtables = ds.immutableMemtables[:i]
	}
}

func (ds *DiskStore) DebugMemtable() {
	ds.memtable.PrintAllRecords()
	utils.Logf("CURRENT SIZE IN BYTES: %d", ds.memtable.sizeInBytes)
}

func deepCopyMemtable(memtable *Memtable) *Memtable {
	deepCopy := NewMemtable()
	deepCopy.sizeInBytes = memtable.sizeInBytes

	keys := memtable.data.Keys()
	values := memtable.data.Values()

	for i := range keys {
		deepCopy.data.Put(keys[i], values[i])
	}

	return deepCopy
}

func (ds *DiskStore) Close() bool {
	return true
}
