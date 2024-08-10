package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	//Pretty much used for the 8 bytes that need to be read ahead of the record's data
	lenWidth = 8
)

type store struct {
	*os.File
	mu sync.Mutex
	buf *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error){
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	return &store{
		File: f,
		size: uint64(fi.Size()),
		buf: bufio.NewWriter(f),
	}, nil

}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error){
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size

	// writes the size of the data (8 bytes), so that we know how much to read / skip by
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, nil
	}

	//buffered writer reduces number of sys calls than directly to file
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, nil
	}

	// the length of the bytes added from adding the length of the data above and the data itself
	w += lenWidth
	s.size += uint64(w)

	return uint64(w), pos, nil
}

func (s* store) Read(pos uint64) ([]byte, error){
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil{
		return nil, err
	} 

	// Reads the 8 bytes (the unit64) that are written as a size to see how much needs to be read next
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// Reads the amount of bytes that was previously specified
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos + lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}


func (s *store) ReadAt(p []byte, off int64) (int, error){
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil{
		return 0, err
	} 

	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}