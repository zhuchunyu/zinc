package core

import "github.com/goccy/go-json"

func (index *Index) SetSourceData(docID string, sourceDoc map[string]interface{}) error {
	jdoc, err := json.Marshal(sourceDoc)
	if err != nil {
		return err
	}
	return index.SourceStorager.Set(docID, jdoc)
}

func (index *Index) GetSourceData(docID string) ([]byte, error) {
	return index.SourceStorager.Get(docID)
}

func (index *Index) DeleteSourceData(docID string) error {
	return index.SourceStorager.Delete(docID)
}
