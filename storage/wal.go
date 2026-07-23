package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	raftproto "raft-kv/proto"
)

// FsyncObserver is a callback to record fsync performance without circular dependencies.
var FsyncObserver func(durationMs float64)

// WAL manages the write-ahead log file.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

// OpenWAL opens or creates a WAL file with restrictive 0600 permissions.
func OpenWAL(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory for WAL: %w", err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		file: file,
		path: path,
	}, nil
}

// AppendState writes the Raft metadata state to the WAL.
func (w *WAL) AppendState(term int64, votedFor string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &raftproto.WALRecord{
		Record: &raftproto.WALRecord_State{
			State: &raftproto.StateMetadata{
				Term:     term,
				VotedFor: votedFor,
			},
		},
	}
	return w.writeRecord(rec)
}

// AppendEntry writes a Raft log entry to the WAL.
func (w *WAL) AppendEntry(entry *raftproto.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &raftproto.WALRecord{
		Record: &raftproto.WALRecord_Entry{
			Entry: entry,
		},
	}
	return w.writeRecord(rec)
}

// AppendTruncate writes a truncation marker to the WAL.
func (w *WAL) AppendTruncate(index int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &raftproto.WALRecord{
		Record: &raftproto.WALRecord_TruncateIndex{
			TruncateIndex: index,
		},
	}
	return w.writeRecord(rec)
}

// Replay reads the WAL from the start and returns the reconstructed state.
func (w *WAL) Replay() (int64, string, []*raftproto.LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var term int64
	var votedFor string
	var entries []*raftproto.LogEntry

	if _, err := w.file.Seek(0, 0); err != nil {
		return 0, "", nil, fmt.Errorf("failed to seek WAL: %w", err)
	}

	for {
		lenBuf := make([]byte, 4)
		_, err := io.ReadFull(w.file, lenBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return 0, "", nil, fmt.Errorf("failed to read record length: %w", err)
		}

		length := binary.BigEndian.Uint32(lenBuf)

		crcBuf := make([]byte, 4)
		if _, err := io.ReadFull(w.file, crcBuf); err != nil {
			return 0, "", nil, fmt.Errorf("failed to read record crc: %w", err)
		}
		crc := binary.BigEndian.Uint32(crcBuf)

		data := make([]byte, length)
		if _, err := io.ReadFull(w.file, data); err != nil {
			return 0, "", nil, fmt.Errorf("failed to read record data: %w", err)
		}

		if crc32.ChecksumIEEE(data) != crc {
			return 0, "", nil, fmt.Errorf("WAL corruption: CRC32 mismatch")
		}

		var rec raftproto.WALRecord
		if err := proto.Unmarshal(data, &rec); err != nil {
			return 0, "", nil, fmt.Errorf("failed to unmarshal WAL record: %w", err)
		}

		switch r := rec.Record.(type) {
		case *raftproto.WALRecord_State:
			term = r.State.Term
			votedFor = r.State.VotedFor
		case *raftproto.WALRecord_Entry:
			idx := r.Entry.Index
			found := false
			for i, e := range entries {
				if e.Index == idx {
					entries = append(entries[:i], r.Entry)
					found = true
					break
				}
			}
			if !found {
				entries = append(entries, r.Entry)
			}
		case *raftproto.WALRecord_TruncateIndex:
			truncIdx := r.TruncateIndex
			var newEntries []*raftproto.LogEntry
			for _, e := range entries {
				if e.Index < truncIdx {
					newEntries = append(newEntries, e)
				}
			}
			entries = newEntries
		}
	}

	return term, votedFor, entries, nil
}

// Reset truncates the WAL file completely.
func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		w.file.Close()
	}

	if err := os.Truncate(w.path, 0); err != nil {
		return fmt.Errorf("failed to truncate WAL file: %w", err)
	}

	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL file after truncate: %w", err)
	}
	w.file = file
	return nil
}

// Close closes the WAL file descriptor.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}

func (w *WAL) writeRecord(rec *raftproto.WALRecord) error {
	data, err := proto.Marshal(rec)
	if err != nil {
		return fmt.Errorf("failed to marshal WAL record: %w", err)
	}

	length := uint32(len(data))
	crc := crc32.ChecksumIEEE(data)

	buf := make([]byte, 8+length)
	binary.BigEndian.PutUint32(buf[0:4], length)
	binary.BigEndian.PutUint32(buf[4:8], crc)
	copy(buf[8:], data)

	if _, err := w.file.Write(buf); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	start := time.Now()
	err = w.file.Sync()
	if FsyncObserver != nil {
		FsyncObserver(float64(time.Since(start).Microseconds()) / 1000.0)
	}
	return err
}
