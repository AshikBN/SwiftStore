package swiftdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	gw "github.com/JyotinderSingh/go-wal"
)

////////////////
//Constants
///////////////

const (
	WALDirectorySuffix = "_wal"
	SSTableFilePrefix  = "sstable_"
	maxLevels          = 6 //maximum number of levels in the lsm tree
)

// Maxumum number of SSTables in each level before compaction is triggered
var maxLevelSSTables = map[int]int{
	0: 4,
	1: 8,
	2: 16,
	3: 32,
	4: 64,
	5: 128,
	6: 256,
}

////////////////
//TYPES
////////////////

type level struct {
	sstables []*SSTable   //SSTables in this level
	mu       sync.RWMutex //[lock 2]: for sstables in this level
}

type KVPair struct {
	Key   string
	Value []byte
}

type LSMTree struct {
	memtable             *Memtable
	mu                   sync.RWMutex   //[lock 1]: for memtable
	maxMemtableSize      int64          //maximum size of memtable before it is flused to the SSTable
	directory            string         //directory where the sstables will be stored
	wal                  *gw.WAL        //WAL for LSMTree
	inRecovery           bool           //true,if LSMTree is recovering entries from the WAL
	levels               []*level       //SSTable in each level
	current_sst_sequence uint64         //sequence number for the next sstable
	compactionChan       chan int       //channel for triggering the compaction at a level
	flushingQueue        []*Memtable    //queue of memtables that needs to flushed to sstables. used to serve reads.
	flushingQueueMu      sync.RWMutex   //[lock 3]: for flushing queue
	flushingChan         chan *Memtable //channel for triggering flushing of memtable to sstable
	ctx                  context.Context
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
}

func Open(directory string, maxMemTableSize int64, recoverFromWAL bool) (*LSMTree, error) {
	ctx, cancel := context.WithCancel(context.Background())

	wal, err := gw.OpenWAL(directory+WALDirectorySuffix, true, 128000, 1000)
	if err != nil {
		cancel()
		return nil, err
	}

	levels := make([]*level, maxLevels)
	for i := 0; i < maxLevels; i++ {
		levels[i] = &level{
			sstables: make([]*SSTable, 0),
		}
	}

	lsm := &LSMTree{
		memtable:             NewMemtable(),
		maxMemtableSize:      maxMemTableSize,
		directory:            directory,
		wal:                  wal,
		inRecovery:           false,
		current_sst_sequence: 0,
		levels:               levels,
		compactionChan:       make(chan int, 100),
		flushingQueue:        make([]*Memtable, 0),
		flushingChan:         make(chan *Memtable, 100),
		ctx:                  ctx,
		cancel:               cancel,
	}

	err = lsm.loadSSTables()
	if err != nil {
		return nil, err
	}

	lsm.wg.Add(2)

	go lsm.backgroundCompaction()
	go lsm.backgroundMemtableFlushing()

	//recover any entries that were written to WAL but not flushed to sstables
	if recoverFromWAL {
		err := lsm.recoverFromWAL()
		if err != nil {
			return nil, err
		}
	}

	return lsm, nil
}

func (l *LSMTree) Close() error {
	l.mu.Lock()
	l.flushingQueueMu.Lock()
	l.flushingQueue = append(l.flushingQueue, l.memtable)
	l.flushingQueueMu.Unlock()

	l.flushingChan <- l.memtable
	l.memtable = NewMemtable()
	l.mu.Unlock()
	l.cancel()
	l.wg.Wait()
	l.wal.Close()
	close(l.flushingChan)
	close(l.compactionChan)
	return nil

}

func (l *LSMTree) Put(key string, value []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	//write to WAL before writing to memtable. we dont need to write to wal if in recovery
	if !l.inRecovery {
		l.wal.WriteEntry(mustMarshal(&WALEntry{
			Key:       key,
			Value:     value,
			Command:   Command_PUT,
			Timestamp: time.Now().UnixNano(),
		}))
	}

	l.memtable.Put(key, value)

	//check if the memtable size reached the max size,if so it will flushed to sstable
	if l.memtable.SizeInBytes() > l.maxMemtableSize {
		l.flushingQueueMu.Lock()
		l.flushingQueue = append(l.flushingQueue, l.memtable)
		l.flushingQueueMu.Unlock()

		l.flushingChan <- l.memtable
		l.memtable = NewMemtable()

	}

	return nil
}

// delete a key-value pair from LSMtree
func (l *LSMTree) Delete(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	//write to WAL before writing to memtable. we dont need to write to wal if in recovery
	if !l.inRecovery {
		l.wal.WriteEntry(mustMarshal(&WALEntry{
			Key:       key,
			Command:   Command_DELETE,
			Timestamp: time.Now().UnixNano(),
		}))
	}

	l.memtable.Delete(key)

	//check if the memtable size reached the max size,if so it will flushed to sstable
	if l.memtable.SizeInBytes() > l.maxMemtableSize {
		l.flushingQueueMu.Lock()
		l.flushingQueue = append(l.flushingQueue, l.memtable)
		l.flushingQueueMu.Unlock()

		l.flushingChan <- l.memtable
		l.memtable = NewMemtable()

	}

	return nil
}

// get the value for a give keu from the LSMTree,
// first search for the key in the memtable,then from the sstable
// return nil if the key is not present
func (l *LSMTree) Get(key string) ([]byte, error) {
	l.mu.RLock()

	value := l.memtable.Get(key)
	if value != nil {
		l.mu.RUnlock()
		return handleValue(value)
	}

	l.flushingQueueMu.RLock()
	for i := len(l.flushingQueue) - 1; i >= 0; i-- {
		value = l.flushingQueue[i].Get(key)
		if value != nil {
			l.flushingQueueMu.RUnlock()
			return handleValue(value)
		}

	}
	l.flushingQueueMu.RUnlock()

	for level := range l.levels {
		l.levels[level].mu.RLock()
		for i := len(l.levels[level].sstables) - 1; i >= 0; i-- {
			value, err := l.levels[level].sstables[i].Get(key)
			if err != nil {
				l.levels[level].mu.RUnlock()
				return nil, err
			}
			if value != nil {
				l.levels[level].mu.RUnlock()
				return handleValue(value)
			}
		}
		l.levels[level].mu.RUnlock()
	}

	return nil, nil

}

// it returns all the entries in the lsmtree that have the keys in the range
// startkey and endkey. the entries are returned in the sorted order of keys
func (l *LSMTree) rangeScan(startKey string, endKey string) ([]KVPair, error) {
	ranges := [][]*LSMEntry{}

	//take all the locks together to ensure a consistent view of LSMTree for the range scan

	l.mu.RLock()
	defer l.mu.Unlock()

	for _, level := range l.levels {
		level.mu.RLock()
		defer level.mu.RUnlock()
	}

	l.flushingQueueMu.RLock()
	defer l.flushingQueueMu.RUnlock()

	entries := l.memtable.RangeScan(startKey, endKey)
	ranges = append(ranges, entries)

	for i := len(l.flushingQueue) - 1; i >= 0; i-- {
		entries = l.flushingQueue[i].RangeScan(startKey, endKey)
		ranges = append(ranges, entries)
	}

	for _, level := range l.levels {
		for i := len(level.sstables) - 1; i >= 0; i-- {
			entries, err := level.sstables[i].RangeScan(startKey, endKey)
			if err != nil {
				return nil, err
			}
			ranges = append(ranges, entries)
		}

	}

	return mergeRanges(ranges), nil

}

// loads all the sstables from disk to memory.
// sorts the sstables by sequence number
// this function is called on startup
func (l *LSMTree) loadSSTables() error {
	err := os.MkdirAll(l.directory, 0755)
	if err != nil {
		return err
	}
	err = l.loadSSTablesFromDisk()
	if err != nil {
		return err
	}
	l.sortSSTablesBySquenceNumber()
	l.initializeCurrentSquenceNumber()

	return nil
}

//loads sstables from the disk
//loads all the files from the directory that have the sstable_ prefix

func (l *LSMTree) loadSSTablesFromDisk() error {
	files, err := os.ReadDir(l.directory)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() || !isSSTableFile(file.Name()) {
			continue
		}

		sstable, err := openSSTable(l.directory + "/" + file.Name())
		if err != nil {
			return err
		}
		level := l.getLevelFromSSTableFilename(sstable.file.Name())
		l.levels[level].sstables = append(l.levels[level].sstables, sstable)

	}
	return nil
}

// get the level from the sstable filename
//
//ex:sstable_0_123->level=0
func (l *LSMTree) getLevelFromSSTableFilename(filename string) int {
	//directory+"/"+sstable_+level(single digit)+_123
	levelStr := filename[len(l.directory)+1+len(SSTableFilePrefix) : len(l.directory)+2+len(SSTableFilePrefix)]
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		panic(err)
	}
	return level
}

// sort sstables by squence number
//
//ex:sstable_0_1,sstable_0_2,sstable_0_3
func (l *LSMTree) sortSSTablesBySquenceNumber() {
	for _, level := range l.levels {
		sort.Slice(level.sstables, func(i int, j int) bool {
			iSquence := l.getSequenceNumber(level.sstables[i].file.Name())
			jSquence := l.getSequenceNumber(level.sstables[j].file.Name())
			return iSquence < jSquence
		})
	}

}

func (l *LSMTree) getSequenceNumber(filename string) uint64 {
	//directory+"/"+sstable_+level(single digit)+_123
	sequenceStr := filename[len(l.directory)+1+len(SSTableFilePrefix)+2:]
	sequence, err := strconv.ParseUint(sequenceStr, 10, 64)
	if err != nil {
		panic(err)
	}
	return sequence
}

func (l *LSMTree) initializeCurrentSquenceNumber() error {
	var maxSquence uint64
	for _, level := range l.levels {
		squence := l.getSequenceNumber(level.sstables[len(level.sstables)-1].file.Name())
		if squence > maxSquence {
			maxSquence = squence
		}
	}
	atomic.StoreUint64(&l.current_sst_sequence, maxSquence)
	return nil

}

// continuously listens to the compactionChan for levels that needs to compacted
// runs a tiered compaction on the lsmtree
// when contexxt is cancelled,return?
func (l *LSMTree) backgroundCompaction() error {
	defer l.wg.Done()
	for {
		select {
		case <-l.ctx.Done():
			readyToExit := l.checkAndTriggerCompaction()
			if readyToExit {
				return nil
			}
		case compactionCandidate := <-l.compactionChan:
			l.compactLevel(compactionCandidate)
		}
	}

}

// continuously listen to flusing channel for memetables to flush
// flush the memtable to sstable if received
// return when the context is cancelled?
func (l *LSMTree) backgroundMemtableFlushing() error {
	for {
		select {
		case <-l.ctx.Done():
			if len(l.flushingChan) == 0 {
				return nil
			}
		case memtable := <-l.flushingChan:
			l.flushMemtable(memtable)
		}
	}
}

// check if all the levels have less than max allowed sstables
// if any level has more than max allowed sstables,trigger compaction
// return true only if all the levels are ready to exit,false otherwise
func (l *LSMTree) checkAndTriggerCompaction() bool {
	readyToExit := true
	for idx, level := range l.levels {
		level.mu.RLock()
		if maxLevelSSTables[idx] < len(level.sstables) {
			l.compactionChan <- idx
			readyToExit = false
		}
		level.mu.RUnlock()
	}
	return readyToExit
}

func (l *LSMTree) compactLevel(compactionCandidate int) error {
	//we dont need to compace the sstables in the last level
	if compactionCandidate == maxLevels-1 {
		return nil
	}

	l.levels[compactionCandidate].mu.RLock()

	if len(l.levels[compactionCandidate].sstables) < maxLevelSSTables[compactionCandidate] {
		l.levels[compactionCandidate].mu.RUnlock()
		return nil
	}

	//1.get the iterator for all the sstables in this level
	_, iterators := l.getSSTableHandlesAtLevel(compactionCandidate)

	//we can release the lock on the level while we processs the sstable
	//this is safe becase sstables are immutable, and can only be deleted by this func and
	//this function is single threaded
	l.levels[compactionCandidate].mu.RUnlock()

	//2.merge all the sstables into new sstable
	mergedSSTable, err := l.mergeSSTables(iterators, compactionCandidate+1)
	if err != nil {
		return err
	}

	l.levels[compactionCandidate].mu.Lock()
	l.levels[compactionCandidate+1].mu.Lock()

	//3.delete old sstables
	l.deleteSSTablesAtLevel(compactionCandidate, iterators)

	//4.add new sstable to the next level
	l.addSSTableAtLevel(mergedSSTable, compactionCandidate+1)

	l.levels[compactionCandidate].mu.Unlock()
	l.levels[compactionCandidate+1].mu.Unlock()

	return nil
}

// flush memtable to on-disk sstable
func (l *LSMTree) flushMemtable(memtable *Memtable) {
	if memtable.size == 0 {
		return
	}
	//get the filename for the new sstable
	atomic.AddUint64(&l.current_sst_sequence, 1)
	sstableFilename := l.getSSTableFilename(0)

	//serialize and store the sstable on the disk with filename
	sstable, err := SerializeToSSTable(memtable.GetEntries(), sstableFilename)
	if err != nil {
		panic(err)
	}

	l.levels[0].mu.Lock()
	l.flushingQueueMu.Lock()

	//add the entry to the wal
	l.wal.CreateCheckpoint(mustMarshal(&WALEntry{
		Key:       sstableFilename,
		Command:   Command_WRITE_SST,
		Timestamp: time.Now().UnixNano(),
	}))

	//add the new sstable to level 0
	l.levels[0].sstables = append(l.levels[0].sstables, sstable)
	l.flushingQueue = l.flushingQueue[1:] //?

	l.levels[0].mu.Unlock()
	l.flushingQueueMu.Unlock()

	l.compactionChan <- 0
}

// read all the entries from WAL
// this will give us all the entries which are written to memtable but not flushed to sstable
func (l *LSMTree) recoverFromWAL() error {
	l.inRecovery = true
	defer func() {
		l.inRecovery = false
	}()
	//read all the entries from the last checkpoint
	entries, err := l.readEntriesFromWAL()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		err = l.processWALEntry(entry)
		if err != nil {
			return err
		}
	}
	return nil

}

func (l *LSMTree) readEntriesFromWAL() ([]*gw.WAL_Entry, error) {
	entries, err := l.wal.ReadAllFromOffset(-1, true)
	if err != nil {
		//Attempt to repair the WAL
		_, err = l.wal.Repair()
		if err != nil {
			return nil, err
		}
		entries, err = l.wal.ReadAllFromOffset(-1, true)
		if err != nil {
			return nil, err
		}
	}
	return entries, nil
}

// this function is used to recover form WAL
// reads the WAL entry and perform the corresponding operation on the lsmtree?
func (l *LSMTree) processWALEntry(entry *gw.WAL_Entry) error {
	if entry.GetIsCheckpoint() {
		//we may used this entry in the future to recover from more sophisticated failures
		return nil
	}
	walEntry := &WALEntry{}
	mustUnmarshal(entry.GetData(), walEntry)
	switch walEntry.Command {
	case Command_PUT:
		return nil //?
	case Command_DELETE:
		return nil
	case Command_WRITE_SST:
		return errors.New("unexpected write sst command to WAL")
	default:
		return errors.New("unknown command in WAL")
	}
}

// get sstables and iterators at a level
func (l *LSMTree) getSSTableHandlesAtLevel(level int) ([]*SSTable, []*SSTableIterator) {
	sstables := l.levels[level].sstables
	iterators := make([]*SSTableIterator, len(sstables))

	for i, sstable := range sstables {
		iterator := sstable.Front()
		iterators[i] = iterator
	}
	return sstables, iterators
}

// delete sstables identified by iterators
func (l *LSMTree) deleteSSTablesAtLevel(level int, iterators []*SSTableIterator) {
	l.levels[level].sstables = l.levels[level].sstables[len(iterators):]
	for _, iterator := range iterators {
		err := os.Remove(iterator.s.file.Name())
		if err != nil {
			panic(err)
		}
	}
}

func (l *LSMTree) addSSTableAtLevel(sstable *SSTable, level int) {
	l.levels[level].sstables = append(l.levels[level].sstables, sstable)
	//send singal to ssable that new sstable has been created
	l.compactionChan <- level
}

// runs merge on iterators and creates new sstable from the merged entries
func (l *LSMTree) mergeSSTables(iterator []*SSTableIterator, targetLevel int) (*SSTable, error) {
	mergedEntries := mergeIterators(iterator)
	sstableFilename := l.getSSTableFilename(targetLevel)
	sstable, err := SerializeToSSTable(mergedEntries, sstableFilename)
	if err != nil {
		return nil, err
	}

	return sstable, nil

}

// get the filename for the next SSTable
func (l *LSMTree) getSSTableFilename(level int) string {
	return fmt.Sprintf("%s/%s%d_%d", l.directory, SSTableFilePrefix, level, atomic.LoadUint64(&l.current_sst_sequence))
}
