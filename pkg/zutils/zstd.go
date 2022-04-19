/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zutils

import (
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"
)

var (
	decoder *zstd.Decoder
	decOnce sync.Once
)

// ZSTDDecompress decompresses a block using ZSTD algorithm.
func ZSTDDecompress(dst, src []byte) ([]byte, error) {
	decOnce.Do(func() {
		var err error
		decoder, err = zstd.NewReader(nil)
		if err != nil {
			log.Fatal().Msg(err.Error())
		}
	})
	return decoder.DecodeAll(src, nil)
}

// ZSTDCompress compresses a block using ZSTD algorithm.
func ZSTDCompress(f io.Writer, src []byte, compressionLevel int) (int, error) {
	level := zstd.EncoderLevelFromZstd(compressionLevel)
	encoder, err := zstd.NewWriter(f, zstd.WithEncoderLevel(level))
	if err != nil {
		return 0, err
	}
	defer encoder.Close()
	return encoder.Write(src)
}
