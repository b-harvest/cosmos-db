package db

import (
	"fmt"
	"time"
)

// memDBBatch operations
type opType int

const (
	opTypeSet opType = iota + 1
	opTypeDelete
)

type operation struct {
	opType
	key   []byte
	value []byte
}

// memDBBatch handles in-memory batching.
type memDBBatch struct {
	db   *MemDB
	ops  []operation
	size int
}

var _ Batch = (*memDBBatch)(nil)

// newMemDBBatch creates a new memDBBatch
func newMemDBBatch(db *MemDB) *memDBBatch {
	return &memDBBatch{
		db:   db,
		ops:  []operation{},
		size: 0,
	}
}

// Set implements Batch.
func (b *memDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.ops == nil {
		return errBatchClosed
	}
	b.size += len(key) + len(value)
	b.ops = append(b.ops, operation{opTypeSet, key, value})
	return nil
}

// Delete implements Batch.
func (b *memDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.ops == nil {
		return errBatchClosed
	}
	b.size += len(key)
	b.ops = append(b.ops, operation{opTypeDelete, key, nil})
	return nil
}

// Write implements Batch.
func (b *memDBBatch) Write() error {
	if b.ops == nil {
		return errBatchClosed
	}
	tFormat := "15:04:05.000"
	fmt.Printf("[%s]memDBBatch.Write:: call db.mtx.Lock()\n", time.Now().Format(tFormat))
	b.db.mtx.Lock()
	fmt.Printf("[%s]memDBBatch.Write:: acquire db.mtx.Lock()\n", time.Now().Format(tFormat))
	defer b.db.mtx.Unlock()

	fmt.Printf("[%s]memDBBatch.Write:: run operations\n", time.Now().Format(tFormat))
	setCnt, delCnt := 0, 0
	for _, op := range b.ops {
		switch op.opType {
		case opTypeSet:
			b.db.set(op.key, op.value)
			setCnt++
		case opTypeDelete:
			b.db.delete(op.key)
			delCnt++
		default:
			return fmt.Errorf("unknown operation type %v (%v)", op.opType, op)
		}
	}
	fmt.Printf("[%s]memDBBatch.Write:: done operations. setCnt=%d, delCnt=%d\n", time.Now().Format(tFormat), setCnt, delCnt)

	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	return b.Close()
}

// WriteSync implements Batch.
func (b *memDBBatch) WriteSync() error {
	return b.Write()
}

// Close implements Batch.
func (b *memDBBatch) Close() error {
	b.ops = nil
	b.size = 0
	return nil
}

// GetByteSize implements Batch
func (b *memDBBatch) GetByteSize() (int, error) {
	if b.ops == nil {
		return 0, errBatchClosed
	}
	return b.size, nil
}
