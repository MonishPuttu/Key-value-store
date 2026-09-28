package engine

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Write-ahead log.
//
// Every write batch is appended to the WAL *before* it is applied to the
// memtable, so an acknowledged write survives a crash: on restart the WAL is
// replayed into a fresh memtable.
//
// Record format (little endian):
//
//	+----------+----------+-------------------+
//	| crc32 u32| len u32  | payload (len B)   |
//	+----------+----------+-------------------+
//
// The CRC (Castagnoli) covers the payload. A torn write at the tail (crash
// in the middle of an append) fails the length or CRC check; replay stops
// there, which is safe because that batch was never acknowledged.

var crcTable = crc32.MakeTable(crc32.Castagnoli)

const walHeaderSize = 8

type walWriter struct {
	f    *os.File
	bw   *bufio.Writer
	num  uint64
	size int64
}

func createWAL(path string, num uint64) (*walWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &walWriter{f: f, bw: bufio.NewWriterSize(f, 64<<10), num: num}, nil
}

// append writes one record; if sync, it is fsynced before returning.
func (w *walWriter) append(payload []byte, sync bool) error {
	var hdr [walHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], crc32.Checksum(payload, crcTable))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(payload)))
	if _, err := w.bw.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.bw.Write(payload); err != nil {
		return err
	}
	// Always hand the bytes to the OS so a process crash cannot lose them;
	// fsync additionally protects against power loss / kernel crash.
	if err := w.bw.Flush(); err != nil {
		return err
	}
	w.size += int64(walHeaderSize + len(payload))
	if sync {
		return w.f.Sync()
	}
	return nil
}

func (w *walWriter) close() error {
	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	if err := w.f.Sync(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

// errTornRecord marks an incomplete or corrupt record at the end of a log.
var errTornRecord = errors.New("wal: torn or corrupt record")

// replayWAL calls fn for every intact record in the file, in order. It
// returns the number of records and whether the log ended with a torn record.
func replayWAL(path string, fn func(payload []byte) error) (n int, torn bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 64<<10)
	var hdr [walHeaderSize]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return n, false, nil
			}
			return n, true, nil // partial header
		}
		crc := binary.LittleEndian.Uint32(hdr[0:4])
		length := binary.LittleEndian.Uint32(hdr[4:8])
		if length > 256<<20 {
			return n, true, nil
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			return n, true, nil
		}
		if crc32.Checksum(payload, crcTable) != crc {
			return n, true, nil
		}
		if err := fn(payload); err != nil {
			return n, false, fmt.Errorf("wal %s record %d: %w", path, n, err)
		}
		n++
	}
}
