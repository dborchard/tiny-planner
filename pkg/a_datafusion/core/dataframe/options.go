package dataframe

import "tiny_planner/pkg/core/common"

type WriteOptions struct {
	Overwrite        bool
	SingleFileOutput bool
	Compression      common.CompressionTypeVariant
}

func NewWriteOptions() *WriteOptions {
	return &WriteOptions{
		Overwrite:        false,
		SingleFileOutput: false,
		Compression:      common.UNCOMPRESSED,
	}
}

func (d *WriteOptions) WithOverwrite(overwrite bool) *WriteOptions {
	d.Overwrite = overwrite
	return d
}

func (d *WriteOptions) WithSingleFileOutput(singleFileOutput bool) *WriteOptions {
	d.SingleFileOutput = singleFileOutput
	return d
}

func (d *WriteOptions) WithCompression(compression common.CompressionTypeVariant) *WriteOptions {
	d.Compression = compression
	return d
}
