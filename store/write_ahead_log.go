package store

import (
	"bytes"
	"os"

	"github.com/aharshit/Distributed-Key-Value-Cache/utils"
)

const WALBatchThreshold = 1024 * 1024 * 3

type writeAheadLog struct {
	file     *os.File
	opsBatch []byte
	size     int
}

func (w *writeAheadLog) clearBatch() {
	w.opsBatch = []byte{}
	w.size = 0
}

func (w *writeAheadLog) appendWALOperation(op Operation, record *Record) error {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(op))

	if encodeErr := record.EncodeKV(buf); encodeErr != nil {
		return utils.ErrEncodingKVFailed
	}

	w.opsBatch = append(w.opsBatch, buf.Bytes()...)
	w.size += len(buf.Bytes())

	if w.size >= WALBatchThreshold {
		return w.flushToDisk()
	}

	return nil
}

func (w *writeAheadLog) flushToDisk() error {
	if logErr := utils.WriteToFile(w.opsBatch, w.file); logErr != nil {
		return logErr
	}

	w.clearBatch()
	return nil
}
