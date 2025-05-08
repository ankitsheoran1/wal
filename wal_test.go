package main

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
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
	fmt.Println("===============", err)
	fmt.Println("++++++++++++++++++++", wal)
}
