package compress

import (
	"bytes"
	"io"

	"github.com/pierrec/lz4"
)

// LZ4Decompress decompresses a block using LZ4 algorithm.
func LZ4Decompress(f io.Reader) ([]byte, error) {
	decoder := lz4.NewReader(f)
	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, decoder); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// LZ4Compress compresses a block using LZ4 algorithm.
func LZ4Compress(f io.Writer, src []byte) (int, error) {
	encoder := lz4.NewWriter(f)
	defer encoder.Close()
	return encoder.Write(src)
}
