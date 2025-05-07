package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type WALEntry struct {
	Data       []byte
	Seq        uint64
	CRC        uint32
	CheckPoint bool
}

type WAL struct {
	maxSegments    uint64
	buff           *bufio.Writer
	currSegment    *os.File
	enableSync     bool
	lock           *sync.Mutex
	dir            string
	lastSeq        uint64
	maxFileSz      uint64
	currSegmentIdx uint64
	syncInterval   *time.Timer
}

func dirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func Create(enableSync bool, maxFileSz uint64, maxSegments uint64, dir string) (*WAL, error) {
	//if exist, err := dirExists(dir); exist {
	//	return nil, fmt.Errorf("dir already exist and used by another process %s", err.Error())
	//}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	files, err := filepath.Glob(filepath.Join(dir, "seg_"+"*"))
	if err != nil {
		return nil, err
	}

	var currSegment uint64
	if len(files) > 0 {
		// traverse and find last segment
		currSegment, err = lastSegIdx(files)
		if err != nil {
			return nil, err
		}
	} else {
		// create a segment
		_, err := createSegment(dir, 0)
		if err != nil {
			return nil, err
		}

	}

	fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$$", currSegment)

	file, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("seg_%d", currSegment)), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("________error_________", err)
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	wal := WAL{
		enableSync:     enableSync,
		maxFileSz:      maxFileSz,
		dir:            dir,
		currSegmentIdx: 0,
		maxSegments:    maxSegments,
		lastSeq:        0,
		syncInterval:   time.NewTimer(100),
	}

	entry, err := wal.lastLogSeq()
	if err != nil || entry == nil {
		return nil, err
	}
	wal.lastSeq = entry.Seq

	return &wal, nil

}

func (wal *WAL) lastLogSeq() (*WALEntry, error) {
	currentFile, err := os.OpenFile(filepath.Join(wal.dir, fmt.Sprintf("seg_%d", wal.currSegmentIdx)), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer currentFile.Close()
	var previousSize int32
	var offset int64
	var entry *WALEntry
	for {
		var sz int32
		if err := binary.Read(currentFile, binary.LittleEndian, &sz); err != nil {
			if err == io.EOF {
				if offset == 0 {
					return entry, nil
				}

				if _, err := currentFile.Seek(offset, io.SeekStart); err != nil {
					return nil, err
				}

				data := make([]byte, previousSize)
				if _, err := io.ReadFull(currentFile, data); err != nil {
					return nil, err
				}

				entry, err := verifyAndUnmarshal(data)
				if err != nil {
					return nil, err
				}

				return entry, nil
			}

			return nil, err
		}
		offset, err = currentFile.Seek(0, io.SeekStart)
		previousSize = sz
		if err != nil {
			return nil, err
		}
		if _, err := currentFile.Seek(int64(sz), io.SeekCurrent); err != nil {
			return nil, err
		}
	}

}

func verifyAndUnmarshal(data []byte) (*WALEntry, error) {
	var entry WALEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("not able to parse the entry")
	}
	if !verify(&entry) {
		return nil, fmt.Errorf("CRC mismatch: data may be corrupted")
	}
	return &entry, nil

}

func verify(entry *WALEntry) bool {
	return crc32.ChecksumIEEE(append(entry.Data, byte(entry.Seq))) == entry.CRC
}

func lastSegIdx(files []string) (uint64, error) {
	fmt.Println("************************", files)
	var currSegment uint64
	for _, file := range files {
		_, segment := filepath.Split(file)
		fmt.Println("^^^^^^^^^^^^^seg^^^^^^^^^", segment)
		segID, err := strconv.Atoi(strings.TrimPrefix(segment, "seg_"))
		if err != nil {
			return 0, err
		}
		if currSegment < uint64(segID) {
			currSegment = uint64(segID)
		}
	}

	return currSegment, nil
}

func createSegment(dir string, segIdx uint64) (*os.File, error) {
	path := filepath.Join(dir, fmt.Sprintf("seg_%d", segIdx))
	return os.Create(path)
}

func main() {

}
