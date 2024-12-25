package swiftdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

func buildMetadataAndEntriesBuffer(messages []*LSMEntry) (*BloomFilter, *Index, *bytes.Buffer, error) {
	var bloomFilter *BloomFilter = newBloomFilter(1000000)
	var index []*IndexEntry
	var entriesBuffer *bytes.Buffer = &bytes.Buffer{}
	var currentOffset EntrySize = 0

	for _, message := range messages {
		data := mustMarshal(message)
		entrySize := EntrySize(len(data))

		bloomFilter.Add([]byte(message.Key))
		index = append(index, &IndexEntry{Key: message.Key, Offset: int64(currentOffset)})
		err := binary.Write(entriesBuffer, binary.LittleEndian, entrySize)
		if err != nil {
			return nil, nil, nil, err
		}
		_, err = entriesBuffer.Write(data)
		if err != nil {
			return nil, nil, nil, err
		}
		currentOffset += EntrySize(binary.Size(entrySize)) + EntrySize((entrySize))
	}
	return bloomFilter, &Index{Entries: index}, entriesBuffer, nil

}

func writeSSTable(filename string, bloomFilterData []byte, indexData []byte, entriesBuffer io.Reader) (EntrySize, error) {
	file, err := os.Create(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	var dataOffset EntrySize = 0

	err = binary.Write(file, binary.LittleEndian, EntrySize(len(bloomFilterData)))
	if err != nil {
		return 0, err
	}
	dataOffset += EntrySize(binary.Size(EntrySize(len(bloomFilterData))))

	_, err = file.Write(bloomFilterData)
	if err != nil {
		return 0, err
	}

	dataOffset += EntrySize(len(bloomFilterData))

	err = binary.Write(file, binary.LittleEndian, EntrySize(len(indexData)))
	if err != nil {
		return 0, nil
	}

	dataOffset += EntrySize(binary.Size(EntrySize(len(indexData))))

	_, err = file.Write(indexData)
	if err != nil {
		return 0, nil
	}

	dataOffset += EntrySize(len(indexData))

	_, err = io.Copy(file, entriesBuffer)
	if err != nil {
		return 0, err
	}

	return dataOffset, nil

}

func readDataSize(file *os.File) (EntrySize, error) {
	var size EntrySize
	err := binary.Read(file, binary.LittleEndian, &size)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func readEntryDataFromFile(file *os.File, size EntrySize) ([]byte, error) {
	data := make([]byte, size)
	_, err := file.Read(data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func readSSTableMetadata(file *os.File) (*BloomFilter, *Index, EntrySize, error) {
	var dataOffset EntrySize

	//read the bloomfilter size
	bloomFilterSize, err := readDataSize(file)
	if err != nil {
		return nil, nil, 0, err
	}
	dataOffset += EntrySize(binary.Size(bloomFilterSize))

	//read bloomfilter data
	bloomFilerData := make([]byte, bloomFilterSize)
	bytesRead, err := file.Read(bloomFilerData)
	if err != nil {
		return nil, nil, 0, err
	}
	dataOffset += EntrySize(bytesRead)

	//read index size
	indexSize, err := readDataSize(file)
	if err != nil {
		return nil, nil, 0, err
	}
	dataOffset += EntrySize(binary.Size(indexSize))

	//read index data
	indexData := make([]byte, indexSize)
	bytesRead, err = file.Read(indexData)
	if err != nil {
		return nil, nil, 0, err
	}
	dataOffset += EntrySize(bytesRead)

	//unmarshal the bloomfilter and index
	bloomFilter := &BloomFilter{}
	index := &Index{}

	mustUnmarshal(bloomFilerData, bloomFilter)
	mustUnmarshal(indexData, index)

	return bloomFilter, index, dataOffset, nil

}

// binary search to find the offset of the key
func findOffsetForKey(index []*IndexEntry, key string) (EntrySize, bool) {
	low := 0
	high := len(index) - 1

	for low <= high {
		mid := (low + high) / 2

		if index[mid].Key == key {
			return EntrySize(index[mid].Offset), true
		} else if index[mid].Key > key {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return 0, false

}

// find the offset for the start key in the index
// if the start key does not present in the index,return next greater element from the index
func findStartOffsetForRangeScan(index []*IndexEntry, startKey string) (EntrySize, bool) {
	low := 0
	high := len(index) - 1
	for low <= high {
		mid := (low + high) / 2
		if index[mid].Key == startKey {
			return EntrySize(index[mid].Offset), true
		} else if index[mid].Key < startKey {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if low >= len(index) {
		return 0, false
	}
	return EntrySize(index[low].Offset), true
}
