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
			index.Analyzers, err = zincanalysis.RequestAnalyzer(index.Settings.Analysis)
			if err != nil {
				return errors.New(errors.ErrorTypeRuntimeException, "parse stored analysis error").Cause(err)
			}
		}

		// load index data
		if err := OpenIndexWriter(index); err != nil {
			return err
		}

		// load in memory
		ZINC_INDEX_LIST.Add(index)
	}

	return nil
}

func ReopenIndex(indexName string) error {
	index, ok := ZINC_INDEX_LIST.Get(indexName)
	if !ok {
		return errors.New(errors.ErrorTypeRuntimeException, "index not found")
	}
	if err := index.Close(); err != nil {
		return err
	}
	return OpenIndexWriter(index)
}

func OpenIndexWriter(index *Index) error {
	var err error
	var defaultSearchAnalyzer *analysis.Analyzer
	if index.Analyzers != nil {
		defaultSearchAnalyzer = index.Analyzers["default"]
	}
	index.Writer, err = LoadIndexWriter(index.Name, index.StorageType, defaultSearchAnalyzer)
	if err != nil {
		return errors.New(errors.ErrorTypeRuntimeException, "load index writer error").Cause(err)
	}
	return nil
}
