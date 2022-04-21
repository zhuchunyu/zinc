package compress

import (
	"io"
	"io/ioutil"

	"github.com/golang/snappy"
)

// ZSTDDecompress decompresses a block using ZSTD algorithm.
func SnappyDecompress(f io.Reader) ([]byte, error) {
	decoder := snappy.NewReader(f)
	return ioutil.ReadAll(decoder)
}

// ZSTDCompress compresses a block using ZSTD algorithm.
func SnappyCompress(f io.Writer, src []byte, _ int) (int, error) {
	encoder := snappy.NewBufferedWriter(f)
	defer encoder.Close()
	return encoder.Write(src)
}
