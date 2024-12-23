package swiftdb

// returns true if the filename is SSTable file
// checks the prefix of the filename
func isSSTableFile(filename string) bool {
	return filename[:len(SSTableFilePrefix)] == SSTableFilePrefix
}

// check if the given entry is tombstone,return the value if it is not,
// return nil if it is
func handleValue(value *LSMEntry) ([]byte, error) {
	if value.Command == Command_DELETE {
		return nil, nil
	}
	return value.Value, nil
}
