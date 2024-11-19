package swiftdb

import (
	"time"

	"github.com/huandu/skiplist"
)

// in-memory table that supports reads,writes,deletes and range scans
type Memtable struct {
	data skiplist.SkipList //ordered map to store k-v pairs in memory
	size int64             // size of the memtable in bytes
}

// creating the new memtable
func NewMemtable() *Memtable {
	return &Memtable{
		data: *skiplist.New(skiplist.String),
		size: 0,
	}
}

// insert a key value pair to memtable.not thread safe
func (m *Memtable) Put(key string, value []byte) {
	sizeChange := int64(len(value))
	existingValue := m.data.Get(key)
	if existingValue != nil {
		m.size -= int64(len(existingValue.Value.(*LSMEntry).Value))
	} else {
		sizeChange += int64(len(key))
	}
	entry := getLSMEntry(key, &value, Command_PUT)
	m.data.Set(key, entry) //logn complexity
	m.size += sizeChange
}

// delete the key value pair from the Memtable.(Not thread safe)
func (m *Memtable) Delete(key string) {
	existingEntry := m.data.Get(key)
	if existingEntry != nil {
		m.size -= int64(len(existingEntry.Value.(*LSMEntry).Value))
	} else {
		m.size += int64(len(key))
	}
	m.data.Set(key, getLSMEntry(key, nil, Command_DELETE))
}

// retrieve the value from the memtable(Not thread safe)
func (m *Memtable) Get(key string) *LSMEntry {
	value := m.data.Get(key)
	if value == nil {
		return nil
	}
	//this may written the tombstone entry(command_delete)
	//caller should check the command field to check the entry is deleted
	return value.Value.(*LSMEntry)
}

func (m *Memtable) RangeScan(startKey string, endKey string) []*LSMEntry {

	var results []*LSMEntry

	iter := m.data.Find(startKey)
	for iter != nil {
		if iter.Element().Key().(string) > endKey {
			break
		}
		//we have included the tombstones in the range.the caller will
		//need to check the command field of the lsmentry to determine
		// that entry is tombstone
		entry := iter.Value.(*LSMEntry)
		results = append(results, entry)
		iter = iter.Next()
	}

	return results

}

func (m *Memtable) SizeInBytes() int64 {
	return m.size
}

func (m *Memtable) Clear() {
	m.data.Init()
	m.size = 0
}

func (m *Memtable) getEntries() []*LSMEntry {
	var entries []*LSMEntry
	iter := m.data.Front()

	for iter != nil {
		entry := iter.Value.(*LSMEntry)
		entries = append(entries, entry)
		iter = iter.Next()
	}
	return entries
}

func getLSMEntry(key string, value *[]byte, command Command) *LSMEntry {
	entry := &LSMEntry{
		Key:       key,
		Command:   command,
		Timestamp: time.Now().UnixNano(),
	}
	if value != nil {
		entry.Value = *value
	}
	return entry
}
