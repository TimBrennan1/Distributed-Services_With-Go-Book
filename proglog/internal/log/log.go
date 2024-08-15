package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/TimBrennan1/proglog/api/v1"
)

type originReader struct {
	*store
	off int64
 }
 // Created so we can implement the io.Reader interface an pass it into the multireader later on in the code
 // Also helps to ensure we read from the start at offset 0
 func (o *originReader) Read(p []byte) (int, error){
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)

	return n, err
 }

 type Log struct {
	mu sync.RWMutex

	Dir string
	Config Config

	activeSegment *segment
	segments []*segment
 }


 func NewLog(dir string, c Config) (*Log, error) {
	
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}

	l := &Log{Dir:dir, Config: c}

	return l, l.setup()

 }

 func (l *Log) Append(record *api.Record) (uint64, error){
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)

	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed(){
		err = l.newSegment(off + 1)
	}

	return off, err
 }

 func (l *Log) Read(off uint64) (*api.Record, error){
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment

	for _, segment := range l.segments {

		// the offset we are looking for is in the range of a given segment, that is our segment
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break;
		}
	}

	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)

	}

	return s.Read(off)
 } 

 func (l *Log) LowestOffset() (uint64, error){
	l.mu.RLock()
	defer l.mu.RUnlock()


	return l.segments[0].baseOffset, nil
 }


 func (l *Log) HighestOffset() (uint64, error){
	l.mu.RLock()
	defer l.mu.RUnlock()

	off := l.segments[len(l.segments) - 1].nextOffset

	return off - 1, nil
 }

 func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	//Removes the segments that are older than a given range
	for _,s := range l.segments {
		if s.nextOffset <= lowest + 1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
 }

 // Lets us read the whole log store
 func (l *Log) Reader() io.Reader{
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))

	// Cant append, since when making, we create a slice of nils
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}

	//concatenates the segment stores
	return io.MultiReader(readers...)
 }

 func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
 }

 func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
 }

 func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
 }


 func (l *Log) setup() error {

	//Reading the dirctory with all of our index and store files
	files, err := os.ReadDir(l.Dir)

	if err != nil {
		return err
	}

	var baseOffsets []uint64

	//Trimming the extensions we added and taking the values (aka offsets) of the numbers that we added to the file paths
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	//Sorting the offsedts
	sort.Slice(baseOffsets, func(i, j int) bool{
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++{
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		//Base index contains duplicate for the index and the store, so we skip the dupe
		i++
	}

	// No initial segments
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil

 }

 func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)

	if err != nil {
		return err
	}

	l.segments = append(l.segments, s)
	l.activeSegment = s

	return nil
 }