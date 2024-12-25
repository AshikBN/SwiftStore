package swiftdb

import (
	"io"
	"os"
)

type EntrySize int64

// add comments?
type SSTable struct {
	bloomFilter *BloomFilter
	index       *Index
	file        *os.File
	dataOffset  EntrySize
}

// add comments?
type SSTableIterator struct {
	s     *SSTable
	file  *os.File
	Value *LSMEntry
}

func SerializeToSSTable(messages []*LSMEntry, filename string) (*SSTable, error) {
	bloomFilter, index, entriesBuffer, err := buildMetadataAndEntriesBuffer(messages)
	if err != nil {
		return nil, err
	}
	bloomFilterData := mustMarshal(bloomFilter)
	indexData := mustMarshal(index)

	dataOffset, err := writeSSTable(filename, bloomFilterData, indexData, entriesBuffer)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return &SSTable{bloomFilter: bloomFilter, index: index, file: file, dataOffset: dataOffset}, nil
}

func OpenSSTable(filename string) (*SSTable, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	bloomFilter, index, dataOffset, err := readSSTableMetadata(file)
	if err != nil {
		return nil, err
	}
	return &SSTable{bloomFilter: bloomFilter, index: index, file: file, dataOffset: dataOffset}, nil
}

// reads the value for the given key and returns the value.
// if key not found returns nil
func (s *SSTable) Get(key string) (*LSMEntry, error) {
	is_available := s.bloomFilter.Test([]byte(key))

	if !is_available {
		return nil, nil
	}
	//find from where the entry starts in the entries buffer
	offset, found := findOffsetForKey(s.index.Entries, key)
	if !found {
		return nil, nil
	}

	//from which point in the file,the entry starts
	entryOffset := s.dataOffset + offset

	//setting the offset of the file to place the entry starts
	_, err := s.file.Seek(int64(entryOffset), io.SeekStart)
	if err != nil {
		return nil, err
	}

	//reading the size of entry from file
	lsmEntrySize, err := readDataSize(s.file)
	if err != nil {
		return nil, err
	}
	//reading the entry from the file
	lsmEntryData, err := readEntryDataFromFile(s.file, lsmEntrySize)
	if err != nil {
		return nil, err
	}

	//deserializing the bytes to lsmEntry
	lsmEntry := &LSMEntry{}
	mustUnmarshal(lsmEntryData, lsmEntry)

	return lsmEntry, nil
}

func (s *SSTable) RangeScan(startKey string, endKey string) ([]*LSMEntry, error) {
	startOffset, found := findStartOffsetForRangeScan(s.index.Entries, startKey)
	if !found {
		return nil, nil
	}

	//setting the offset of the file to place the entry starts
	entryOffset := startOffset + s.dataOffset
	_, err := s.file.Seek(int64(entryOffset), io.SeekStart)
	if err != nil {
		return nil, err
	}

	var lsmEntries []*LSMEntry
	for {
		//reading the size of entry from file
		lsmEntrySize, err := readDataSize(s.file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		//reading the entry from the file
		lsmEntryData, err := readEntryDataFromFile(s.file, lsmEntrySize)
		if err != nil {
			return nil, err
		}

		//deserializing the bytes to lsmEntry
		lsmEntry := &LSMEntry{}
		mustUnmarshal(lsmEntryData, lsmEntry)

		if lsmEntry.Key > endKey {
			break
		}

		lsmEntries = append(lsmEntries, lsmEntry)

	}
	return lsmEntries, nil
}

// return all the lsmEntries in the sstable
func (s *SSTable) GetEntries() ([]*LSMEntry, error) {
	_, err := s.file.Seek(int64(s.dataOffset), io.SeekStart)
	if err != nil {
		return nil, err
	}

	var lsmEntries []*LSMEntry
	for {

		//reading the size of entry from file
		lsmEntrySize, err := readDataSize(s.file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		//reading the entry from the file
		lsmEntryData, err := readEntryDataFromFile(s.file, lsmEntrySize)
		if err != nil {
			return nil, err
		}

		//deserializing the bytes to lsmEntry
		lsmEntry := &LSMEntry{}
		mustUnmarshal(lsmEntryData, lsmEntry)

		lsmEntries = append(lsmEntries, lsmEntry)

	}
	return lsmEntries, nil
}

// returning an iterator for the SSTable
// iterator is positioned at the beginning of the sstable
func (s *SSTable) Front() *SSTableIterator {
	//opening the new file handle for the iterator
	file, err := os.Open(s.file.Name())
	if err != nil {
		return nil
	}

	iterator := &SSTableIterator{s: s, file: file, Value: &LSMEntry{}}

	_, err = iterator.file.Seek(int64(s.dataOffset), io.SeekStart)
	if err != nil {
		panic(err)
	}

	lsmEntrySize, err := readDataSize(iterator.file)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}

	lsmEntryData, err := readEntryDataFromFile(iterator.file, lsmEntrySize)
	if err != nil {
		panic(err)
	}

	mustUnmarshal(lsmEntryData, iterator.Value)

	return iterator
}

func (iterator *SSTableIterator) Next() *SSTableIterator {

	lsmEntrySize, err := readDataSize(iterator.file)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}

	lsmEntryData, err := readEntryDataFromFile(iterator.file, lsmEntrySize)
	if err != nil {
		panic(err)
	}

	iterator.Value = &LSMEntry{}
	mustUnmarshal(lsmEntryData, iterator.Value)

	return iterator
}

func (s *SSTable) Close() error {
	return s.file.Close()
}

func (iterator *SSTableIterator) Close() {
	iterator.file.Close()
}
