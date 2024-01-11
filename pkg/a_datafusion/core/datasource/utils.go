package datasource

import (
	"encoding/csv"
	"github.com/apache/arrow/go/v12/arrow"
	"os"
	"path/filepath"
)

// ReadAllFiles reads all files in the given directory
func ReadAllFiles(dirPath string) ([]string, error) {
	var files []string
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

// InferArrowSchemaFromCSV infers an Arrow schema from a CSV file
func InferArrowSchemaFromCSV(filePath string) (schema *arrow.Schema, err error) {
	// 1. Open File
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err = file.Close()
	}(file)

	// 2. Read CSV
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	// 3. Create Arrow Schema
	fields := make([]arrow.Field, len(header))
	for i, name := range header {
		fields[i] = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64}
	}

	schema = arrow.NewSchema(fields, nil)
	return schema, nil
}
