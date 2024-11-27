package swiftdb

import (
	"container/heap"

	"github.com/huandu/skiplist"
)

// Heap entry for the k-way merge algorithm
type heapEntry struct {
	entry     *LSMEntry
	listIndex int //index of entry source
	idx       int //index of entry in the list
	iterator  *SSTableIterator
}

// Heap implementation for k-way merge algorithm
type mergeHeap []heapEntry

func (h mergeHeap) Len() int {
	return len(h)
}

// ?
func (h mergeHeap) Less(i, j int) bool {
	return h[i].entry.Timestamp < h[j].entry.Timestamp
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x interface{}) {
	h.Push(x.(heapEntry))
}

// pop the entry from the heap
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func mergeIterators(iterators []*SSTableIterator) []*LSMEntry {
	minHeap := &mergeHeap{}
	heap.Init(minHeap)

	var results []*LSMEntry

	//keeps track of the most recent entry for each key, in the sorted keys
	seen := skiplist.New(skiplist.String)

	//add all the sstable iterators(with first lsm entry) to the heap(sorted based on entry timestamp)
	for _, iterator := range iterators {
		if iterator == nil {
			continue
		}
		heap.Push(minHeap, heapEntry{entry: iterator.Value, iterator: iterator})
	}
	//creating an ordered skiplist to store unique key and heapEntry and filtering the deleted keys
	for minHeap.Len() > 0 {
		//get the entry with minimum timestamp
		minEntry := heap.Pop(minHeap).(heapEntry)
		previousValue := seen.Get(minEntry.entry.Key)

		//check if the entry already present in skiplist
		//if not add the pair to list
		//if present, make sure to update to most recent value(recent timestamp)
		if previousValue != nil {
			if previousValue.Value.(heapEntry).entry.Timestamp < minEntry.entry.Timestamp {
				seen.Set(minEntry.entry.Key, minEntry)
			}
		} else {
			seen.Set(minEntry.entry.Key, minEntry)
		}

		//get the next value from the current sstable iterator
		if minEntry.iterator.Next() != nil {
			nextEntry := minEntry.iterator.Value
			heap.Push(minHeap, heapEntry{entry: nextEntry, iterator: minEntry.iterator})
		}

	}
	//add the entrys which are not deleted to result
	iter := seen.Front()
	for iter != nil {
		entry := iter.Value.(heapEntry)
		if entry.entry.Command == Command_DELETE {
			iter = iter.Next()
			continue
		}
		results = append(results, entry.entry)
		iter = iter.Next()
	}

	return results

}
