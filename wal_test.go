package main

import (
	"fmt"
	"testing"
)

func TestCreate(t *testing.T) {
	wal, err := Create(true, 1024, 10, "test")
	fmt.Println("===============", err)
	fmt.Println("++++++++++++++++++++", wal)
}
