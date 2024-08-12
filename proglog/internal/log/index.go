package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	//bytes of space
	offsetWidth uint64 = 4
	posWidth uint64 = 8

	entWidth = offsetWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex( f *os.File, c Config) (*index, error){
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())

	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	// We want the file size to be max index size
	// We resize now because we can't later after they are memory mapped
	// We still have the index location, since we can use size and the current location is at size before resizing
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil{
		return nil, err
	}

	// mmap puts the file into the virtual address space of the calling process
	// This gets from disk and allows us to make changes and then it gets re-written if the memory is "dirtied"
	// See https://man7.org/linux/man-pages/man2/mmap.2.html (this is for C, but the same concept applies)
	if idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}

	return idx, nil
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error){
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		// reads the last offset
		out = uint32((i.size / entWidth) -1)
	} else {
		// Reads at the offset of the inputted variable
		out = uint32(in)
	}

	// Determines the start of next entry. offset is first 4 bytes, position is next 8 bytes
	pos = uint64(out) * entWidth

	// no more entries to view
	if i.size < pos + entWidth {
		return 0, 0, io.EOF
	}

	//gets offset
	out = enc.Uint32(i.mmap[pos : pos + offsetWidth])
	//gets position
	pos = enc.Uint64(i.mmap[pos + offsetWidth : pos + entWidth])

	return out, pos, nil
}

func (i *index) Write (off uint32, pos uint64) error {
	//Trying to write into a full file
	if uint64(len(i.mmap)) < i.size + entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size : i.size + offsetWidth], off)
	enc.PutUint64(i.mmap[i.size + offsetWidth : i.size + entWidth], pos)

	i.size += entWidth

	return nil
}

func (i *index) Name() string{
	return i.file.Name()
}


func (i *index) Close() error {

	//Flushes changes made in virtual address space into the file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	//Flushes changes made to the file into main memory (or stable memory)
	if err := i.file.Sync(); err != nil {
		return err
	}

	//Truncates the files size to the actual ammount of data inside of it, since we set it to max value on creation...
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return nil
}