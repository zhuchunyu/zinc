//  Copyright (c) 2020 The Bluge Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package directory

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/blugelabs/bluge/index/lock"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/zinclabs/zinc/pkg/startup"
	"github.com/zinclabs/zinc/pkg/zutils/compress"
)

const pidFilename = "bluge.pid"

type LoadMMapFunc func(f lock.LockedFile) (*segment.Data, io.Closer, error)

type CompressDirectory struct {
	path string
	pid  lock.LockedFile

	newDirPerm  os.FileMode
	newFilePerm os.FileMode

	openExclusive func(path string, flag int, perm os.FileMode) (lock.LockedFile, error)
	openShared    func(path string, flag int, perm os.FileMode) (lock.LockedFile, error)
}

// GetCompressConfig returns a bluge config that will store index data with compression
// path: the name of the index to use. It will be a file prefix (folder)
func GetCompressConfig(path string) bluge.Config {
	return bluge.DefaultConfigWithDirectory(func() index.Directory {
		return NewCompressDirectory(path)
	})
}

func NewCompressDirectory(path string) *CompressDirectory {
	return &CompressDirectory{
		path:          path,
		openExclusive: lock.OpenExclusive,
		openShared:    lock.OpenShared,
		newDirPerm:    0700,
		newFilePerm:   0600,
	}
}

func (d *CompressDirectory) exists() (bool, error) {
	_, err := os.Stat(d.path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func (d *CompressDirectory) Setup(readOnly bool) error {
	dirExists, err := d.exists()
	if err != nil {
		return fmt.Errorf("error checking if directory exists '%s': %w", d.path, err)
	}
	if !dirExists {
		if readOnly {
			return fmt.Errorf("readOnly, directory does not exist")
		}
		err = os.MkdirAll(d.path, d.newDirPerm)
		if err != nil {
			return fmt.Errorf("error creating directory '%s': %w", d.path, err)
		}
	}
	return nil
}

func (d *CompressDirectory) List(kind string) ([]uint64, error) {
	dirEntries, err := ioutil.ReadDir(d.path)
	if err != nil {
		return nil, err
	}

	var rv uint64Slice
	for _, dirEntry := range dirEntries {
		if filepath.Ext(dirEntry.Name()) != kind {
			continue
		}
		base := dirEntry.Name()
		base = base[:len(base)-len(kind)]
		var epoch uint64
		epoch, err = strconv.ParseUint(base, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing identifier '%s': %w", base, err)
		}
		rv = append(rv, epoch)
	}

	sort.Sort(sort.Reverse(rv))

	return rv, nil
}

func (d *CompressDirectory) Persist(kind string, id uint64, w index.WriterTo, closeCh chan struct{}) error {
	path := filepath.Join(d.path, d.fileName(kind, id))
	f, err := d.openExclusive(path, os.O_CREATE|os.O_RDWR, d.newFilePerm)
	if err != nil {
		return err
	}

	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(path)
	}

	buf := bytes.NewBuffer(nil)
	_, err = w.WriteTo(buf, closeCh)
	if err != nil {
		cleanup()
		return err
	}

	_, err = d.compressData(f.File(), buf.Bytes())
	if err != nil {
		cleanup()
		return err
	}
	buf.Reset()

	err = f.File().Sync()
	if err != nil {
		cleanup()
		return err
	}

	err = f.Close()
	if err != nil {
		cleanup()
		return err
	}

	return nil
}

func (d *CompressDirectory) Load(kind string, id uint64) (*segment.Data, io.Closer, error) {
	path := filepath.Join(d.path, d.fileName(kind, id))
	f, err := d.openShared(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, nil, err
	}

	data, err := d.deCompressData(f.File())
	if err != nil {
		return nil, nil, fmt.Errorf("error creating data from file: %w", err)
	}

	return segment.NewDataBytes(data), closerFunc(f.Close), nil
}

func (d *CompressDirectory) Remove(kind string, id uint64) error {
	return d.remove(kind, id)
}

func (d *CompressDirectory) Lock() error {
	pidPath := filepath.Join(d.path, pidFilename)
	var err error
	d.pid, err = d.openExclusive(pidPath, os.O_CREATE|os.O_RDWR, d.newFilePerm)
	if err != nil {
		return fmt.Errorf("unable to obtain exclusive access: %w", err)
	}
	err = d.pid.File().Truncate(0)
	if err != nil {
		return fmt.Errorf("error truncating pid file: %w", err)
	}
	_, err = d.pid.File().Write([]byte(fmt.Sprintf("%d\n", os.Getpid())))
	if err != nil {
		return fmt.Errorf("error writing pid: %w", err)
	}
	err = d.pid.File().Sync()
	if err != nil {
		return fmt.Errorf("error syncing pid file: %w", err)
	}
	return err
}

func (d *CompressDirectory) Unlock() error {
	pidPath := filepath.Join(d.path, pidFilename)
	var err error
	err = d.pid.Close()
	if err != nil {
		return fmt.Errorf("error closing pid file: %w", err)
	}
	err = os.RemoveAll(pidPath)
	if err != nil {
		return fmt.Errorf("error removing pid file: %w", err)
	}
	return err
}

func (d *CompressDirectory) Stats() (numFilesOnDisk, numBytesUsedDisk uint64) {
	fileInfos, err := ioutil.ReadDir(d.path)
	if err == nil {
		for _, fileInfo := range fileInfos {
			if !fileInfo.IsDir() {
				numFilesOnDisk++
				numBytesUsedDisk += uint64(fileInfo.Size())
			}
		}
	}
	return numFilesOnDisk, numBytesUsedDisk
}

func (d *CompressDirectory) Sync() error {
	dir, err := os.Open(d.path)
	if err != nil {
		return fmt.Errorf("error opening directory for sync: %w", err)
	}
	err = dir.Sync()
	if err != nil {
		_ = dir.Close()
		return fmt.Errorf("error syncing directory: %w", err)
	}
	err = dir.Close()
	if err != nil {
		return fmt.Errorf("error closing directing after sync: %w", err)
	}
	return nil
}

func (d *CompressDirectory) compressData(dst io.Writer, src []byte) (int, error) {
	switch startup.LoadIndexCompressAlgorithm() {
	case compress.LZ4:
		return compress.LZ4Compress(dst, src)
	case compress.ZSTD:
		return compress.ZSTDCompress(dst, src, 3) // ZSTD compression level: 1, 3, 9
	default: // SNAPPY
		return compress.SnappyCompress(dst, src)
	}
}

func (d *CompressDirectory) deCompressData(src *os.File) ([]byte, error) {
	switch startup.LoadIndexCompressAlgorithm() {
	case compress.LZ4:
		return compress.LZ4Decompress(src)
	case compress.ZSTD:
		buf, err := io.ReadAll(src)
		if err != nil {
			return nil, err
		}
		return compress.ZSTDDecompress(buf)
	default: // SNAPPY
		return compress.SnappyDecompress(src)
	}
}

func (d *CompressDirectory) fileName(kind string, id uint64) string {
	return fmt.Sprintf("%012x", id) + kind
}

type uint64Slice []uint64

func (e uint64Slice) Len() int           { return len(e) }
func (e uint64Slice) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e uint64Slice) Less(i, j int) bool { return e[i] < e[j] }

type closerFunc func() error

func (c closerFunc) Close() error {
	return c()
}
