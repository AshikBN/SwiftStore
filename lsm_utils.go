package swiftdb

// returns true if the filename is SSTable file
// checks the prefix of the filename
func isSSTableFile(filename string) bool {
	return filename[:len(SSTableFilePrefix)] == SSTableFilePrefix
}
