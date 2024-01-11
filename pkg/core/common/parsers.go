package common

type CompressionTypeVariant int

const (
	GZIP CompressionTypeVariant = iota
	BZIP2
	XZ
	ZSTD
	UNCOMPRESSED
)
