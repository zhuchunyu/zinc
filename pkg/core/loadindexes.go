/* Copyright 2022 Zinc Labs Inc. and Contributors
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

package core

import (
	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/rs/zerolog/log"

	"github.com/zinclabs/zinc/pkg/errors"
	"github.com/zinclabs/zinc/pkg/metadata"
	zincanalysis "github.com/zinclabs/zinc/pkg/uquery/analysis"
)

func LoadZincIndexesFromMetadata() error {
	indexes, err := metadata.Index.List(0, 0)
	if err != nil {
		return err
	}

	for i := range indexes {
		// cache mappings
		index := new(Index)
		index.Name = indexes[i].Name
		index.StorageType = indexes[i].StorageType
		index.Settings = indexes[i].Settings
		index.Mappings = indexes[i].Mappings
		index.Mappings = indexes[i].Mappings
		log.Info().Msgf("Loading index... [%s:%s]", index.Name, index.StorageType)

		// load index analysis
		if index.Settings != nil && index.Settings.Analysis != nil {
			index.CachedAnalyzers, err = zincanalysis.RequestAnalyzer(index.Settings.Analysis)
			if err != nil {
				return errors.New(errors.ErrorTypeRuntimeException, "parse stored analysis error").Cause(err)
			}
		}

		// load index data
		// var defaultSearchAnalyzer *analysis.Analyzer
		// if index.CachedAnalyzers != nil {
		// 	defaultSearchAnalyzer = index.CachedAnalyzers["default"]
		// }
		// index.Writer, err = LoadIndexWriter(index.Name, index.StorageType, defaultSearchAnalyzer)
		// if err != nil {
		// 	return errors.New(errors.ErrorTypeRuntimeException, "load index writer error").Cause(err)
		// }

		// load index docs count
		index.DocsCount, _ = index.LoadDocsCount()

		// load index size
		index.ReLoadStorageSize()
		// load in memory
		ZINC_INDEX_LIST.Add(index)
	}

	return nil
}

func (index *Index) GetWriter() (*bluge.Writer, error) {
	index.m.RLock()
	w := index.Writer
	index.m.RUnlock()
	if w != nil {
		return w, nil
	}

	index.m.Lock()
	err := index.open(true, 0, 0)
	index.m.Unlock()
	return index.Writer, err
}

func (index *Index) GetReader(timeMin, timeMax int64) (*bluge.Reader, error) {
	var r *bluge.Reader
	var err error
	index.m.RLock()
	if index.Writer != nil {
		r, err = index.Writer.Reader()
	}
	index.m.RUnlock()
	if err != nil {
		return nil, err
	}
	if r != nil {
		return r, nil
	}

	// TODO cache reader

	index.m.Lock()
	err = index.open(false, timeMin, timeMax)
	r = index.Reader.reader
	index.Reader = nil
	index.m.Unlock()
	return r, err
}

func (index *Index) open(write bool, timeMin, timeMax int64) error {
	var defaultSearchAnalyzer *analysis.Analyzer
	if index.CachedAnalyzers != nil {
		defaultSearchAnalyzer = index.CachedAnalyzers["default"]
	}

	if write {
		writer, err := OpenIndexWriter(index.Name, index.StorageType, defaultSearchAnalyzer, 0, 0)
		if err != nil {
			return err
		}
		index.Writer = writer
		return nil
	}

	reader, err := OpenIndexReader(index.Name, index.StorageType, defaultSearchAnalyzer, timeMin, timeMax)
	if err != nil {
		return err
	}
	index.Reader = &IndexReader{
		reader:  reader,
		timeMin: timeMin,
		timeMax: timeMax,
	}
	return nil
}
