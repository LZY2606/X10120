package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Record is one append-only audit entry. Raw is populated only for
// event_received and holds the verbatim original payload bytes.
type Record struct {
	Seq  int64           `json:"seq"`
	At   time.Time       `json:"at"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
	Raw  []byte          `json:"raw,omitempty"`
}

// auditLog is a line-delimited JSON append log. Every committed record is
// fsynced before the in-memory state is updated, so a crash at any point
// leaves the log with either a complete line or no line at all.
type auditLog struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// truncate cuts the file to size bytes, discarding a torn trailing line.
func (l *auditLog) truncate(size int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.w.Flush(); err != nil {
		return err
	}
	if err := l.f.Truncate(size); err != nil {
		return err
	}
	if _, err := l.f.Seek(size, 0); err != nil {
		return err
	}
	l.w.Reset(l.f)
	return nil
}

func openAuditLog(dir string) (*auditLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "audit.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &auditLog{f: f, w: bufio.NewWriterSize(f, 4096)}, nil
}

func (l *auditLog) append(rec *Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := l.w.Write(line); err != nil {
		return err
	}
	if err := l.w.WriteByte('\n'); err != nil {
		return err
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

func (l *auditLog) close() error {
	return l.f.Close()
}

// replay reads every complete record from the log. A torn trailing line
// (process killed mid-write) is ignored rather than aborting the replay,
// because that line was never acknowledged as committed.
func replay(dir string, apply func(Record) (int64, error)) (int64, int64, bool, error) {
	f, err := os.Open(filepath.Join(dir, "audit.log"))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var lastSeq int64
	var lastOffset int64
	var offset int64
	lineNo := 0
	for {
		line, err := r.ReadBytes('\n')
		nextOffset := offset + int64(len(line))
		lineNo++
		if len(line) > 0 {
			var rec Record
			if jerr := json.Unmarshal(line, &rec); jerr != nil {
				if err == io.EOF {
					return lastSeq, lastOffset, true, nil
				}
				return 0, 0, false, fmt.Errorf("audit.log line %d: %w", lineNo, jerr)
			}
			_, aerr := apply(rec)
			if aerr != nil {
				return 0, 0, false, fmt.Errorf("audit.log line %d: %w", lineNo, aerr)
			}
			lastSeq = rec.Seq
			lastOffset = nextOffset
		}
		offset = nextOffset
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, 0, false, err
		}
	}
	return lastSeq, lastOffset, false, nil
}
