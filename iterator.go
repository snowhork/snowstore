package snowstore

import (
	"encoding/gob"
	"os"

	"golang.org/x/xerrors"
)

type Iterator struct {
	files []string
	index int
}

func newIterator(files []string) *Iterator {
	return &Iterator{files: files, index: 0}
}

func (it *Iterator) HasNext() bool {
	return it.index != len(it.files)
}

func (it *Iterator) Next(dst interface{}) error {
	f, err := os.Open(it.files[it.index])
	if err != nil {
		return xerrors.Errorf("%w", err)
	}
	defer f.Close()

	enc := gob.NewDecoder(f)
	if err := enc.Decode(dst); err != nil {
		return xerrors.Errorf("%w", err)
	}

	it.index += 1
	return nil
}
