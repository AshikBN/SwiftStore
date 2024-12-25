package tests

import (
	"os"
	"testing"

	swiftdb "github.com/AshikBN/SwiftDB"
	"github.com/stretchr/testify/assert"
)

func TestLSMTreePut(t *testing.T) {
	t.Parallel()
	dir := "TestLSMTreePut"
	l, err := swiftdb.Open(dir, 1000, true)
	assert.Nil(t, err)

	defer os.Remove(dir)
	defer os.RemoveAll(dir + swiftdb.WALDirectorySuffix)

	// Put a key-value pair into the LSMTree.
	err = l.Put("key", []byte("value"))
	assert.Nil(t, err)

	// Check that the key-value pair exists in the LSMTree.
	value, err := l.Get("key")
	assert.Nil(t, err)
	assert.Equal(t, "value", string(value), "Expected value to be 'value', got '%v'", string(value))

	// // Delete the key-value pair from the LSMTree.
	err = l.Delete("key")
	assert.Nil(t, err)

	// // Check that the key-value pair no longer exists in the LSMTree.
	value, err = l.Get("key")
	assert.Nil(t, err)
	assert.Nil(t, value, "Expected value to be nil, got '%v'", string(value))

	l.Close()

	// // Check that the key-value pair still exists in the LSMTree after closing and
	// // reopening it.
	// l, err = swiftdb.Open(dir, 1000, true)
	// assert.Nil(t, err)

	// value, err = l.Get("key")
	// assert.Nil(t, err)
	// assert.Nil(t, value, "Expected value to be nil, got '%v'", string(value))

	// l.Close()
}
