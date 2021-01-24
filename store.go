package snowstore

import (
	"encoding/gob"
	"io/ioutil"
	"os"
	"path"

	"golang.org/x/xerrors"
)

type SnowStore struct {
	config SnowStoreConfig
}

type SnowStoreConfig struct {
	path string
}

type Person struct {
	ID   int
	Name string
}

func (s *SnowStore) Get(parent, key string, dst interface{}) error {
	f, err := os.Open(s.filePath(parent, key))
	if err != nil {
		if xerrors.Is(err, os.ErrNotExist) || xerrors.Is(err, os.ErrPermission) {
			return ErrEntryNotFound
		}

		return xerrors.Errorf("%w", err)
	}
	defer f.Close()

	enc := gob.NewDecoder(f)
	if err := enc.Decode(dst); err != nil {
		return xerrors.Errorf("%w", err)
	}

	return nil
}

func (s *SnowStore) Set(parent, key string, value interface{}) error {
	if err := os.MkdirAll(s.parentDirPath(parent), 0774); err != nil {
		return xerrors.Errorf("%w", err)
	}
	f, err := os.Create(s.filePath(parent, key))
	if err != nil {
		return xerrors.Errorf("%w", err)
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	if err := enc.Encode(value); err != nil {
		return xerrors.Errorf("%w", err)
	}

	return nil
}

func (s *SnowStore) GetByParent(parent string) (*Iterator, error) {
	if parent == "" {
		return nil, ErrRootParentSpecified
	}

	basedirPath := s.parentDirPath(parent)
	if err := os.MkdirAll(basedirPath, 0774); err != nil {
		return nil, xerrors.Errorf("%w", err)
	}
	fs, err := ioutil.ReadDir(basedirPath)
	if err != nil {
		return nil, xerrors.Errorf("%w", err)
	}

	var filePaths []string
	for _, f := range fs {
		if f.IsDir() {
			continue
		}

		filePaths = append(filePaths, path.Join(basedirPath, f.Name()))
	}

	it := newIterator(filePaths)
	return it, nil
}

func (s *SnowStore) Delete(parent, key string) error {
	err := os.Remove(s.filePath(parent, key))
	if err != nil {
		return xerrors.Errorf("%w", err)
	}

	return nil
}

func (s *SnowStore) DeleteByParent(parent string) error {
	if parent == "" {
		return ErrRootParentSpecified
	}

	if err := os.RemoveAll(s.parentDirPath(parent)); err != nil {
		return xerrors.Errorf("%w", err)
	}

	return nil
}

func (s *SnowStore) filePath(parent, key string) string {
	return path.Join(s.parentDirPath(parent), key)
}

const dataPath = "data"

func (s *SnowStore) parentDirPath(parent string) string {
	return path.Join(s.config.path, dataPath, parent)
}
