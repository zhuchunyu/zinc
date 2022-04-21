package compress

import (
	"io"
	"io/ioutil"

	"github.com/golang/snappy"
)

// SnappyDecompress decompresses a block using Snappy algorithm.
func SnappyDecompress(f io.Reader) ([]byte, error) {
	decoder := snappy.NewReader(f)
	return ioutil.ReadAll(decoder)
}

// SnappyCompress compresses a block using Snappy algorithm.
func SnappyCompress(f io.Writer, src []byte) (int, error) {
	encoder := snappy.NewBufferedWriter(f)
	defer encoder.Close()
	return encoder.Write(src)
}
