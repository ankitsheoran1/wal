package main

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"testing"
)

type OperationType int

const (
	InsertOperation OperationType = iota
	DeleteOperation
)

type Record struct {
	Op    OperationType `json:"op"`
	Key   string        `json:"key"`
	Value []byte        `json:"value"`
}

func TestCreate(t *testing.T) {
	wal, err := Create(true, 1024, 10, "test")
	defer os.RemoveAll("test")
	fmt.Println("======wal is ==============", wal)
	entries := []Record{
		{Key: "key1", Value: []byte("value1"), Op: InsertOperation},
		{Key: "key2", Value: []byte("value2"), Op: InsertOperation},
		{Key: "key3", Op: DeleteOperation},
	}
	for _, entry := range entries {
		marshaledEntry, err := json.Marshal(entry)
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, wal.write(marshaledEntry, false), "Failed to write entry")
	}

	recoveredEntries, err := wal.readAll()
	fmt.Println("++++++++++++++length is ++++++++++++", len(recoveredEntries))
	for entryIndex, entry := range recoveredEntries {
		unMarshalledEntry := Record{}
		assert.NoError(t, json.Unmarshal(entry.Data, &unMarshalledEntry), "Failed to unmarshal entry")
		fmt.Println("========each read entry=======", unMarshalledEntry.Key)

		// Can't use deep equal because of the sequence number
		assert.Equal(t, entries[entryIndex].Key, unMarshalledEntry.Key, "Recovered entry does not match written entry (Key)")
		assert.Equal(t, entries[entryIndex].Op, unMarshalledEntry.Op, "Recovered entry does not match written entry (Op)")
		assert.True(t, reflect.DeepEqual(entries[entryIndex].Value, unMarshalledEntry.Value), "Recovered entry does not match written entry (Value)")
	}
	assert.NoError(t, err, "Failed to recover entries")

	fmt.Println("===============", err)
	fmt.Println("++++++++++++++++++++", wal)
}
