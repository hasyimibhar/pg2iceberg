// Package sink re-exports types from the iceberg package for backward compatibility
// during the incremental migration. These aliases will be removed once all consumers
// import from iceberg directly.
package sink

import "github.com/pg2iceberg/pg2iceberg/iceberg"

// Catalog and storage interfaces.
type Catalog = iceberg.Catalog
type ObjectStorage = iceberg.ObjectStorage

// Catalog types.
type CatalogClient = iceberg.CatalogClient
type TableMetadata = iceberg.TableMetadata
type SnapshotCommit = iceberg.SnapshotCommit
type TableCommit = iceberg.TableCommit

// Storage types.
type S3Client = iceberg.S3Client
type SigV4Transport = iceberg.SigV4Transport
type S3ReaderAt = iceberg.S3ReaderAt

// Writer types.
type ParquetWriter = iceberg.ParquetWriter
type FileChunk = iceberg.FileChunk
type RollingWriter = iceberg.RollingWriter

// Manifest types.
type DataFileInfo = iceberg.DataFileInfo
type ManifestEntry = iceberg.ManifestEntry
type ManifestFileInfo = iceberg.ManifestFileInfo

// Partition types.
type PartitionSpec = iceberg.PartitionSpec
type PartitionField = iceberg.PartitionField

// Re-export constructor functions.
var (
	NewCatalogClient      = iceberg.NewCatalogClient
	NewSigV4Transport     = iceberg.NewSigV4Transport
	NewS3Client           = iceberg.NewS3Client
	KeyFromURI            = iceberg.KeyFromURI
	NewDataWriter         = iceberg.NewDataWriter
	NewDeleteWriter       = iceberg.NewDeleteWriter
	NewRollingDataWriter  = iceberg.NewRollingDataWriter
	NewRollingDeleteWriter = iceberg.NewRollingDeleteWriter
	BuildPartitionSpec    = iceberg.BuildPartitionSpec
	EventsTableName       = iceberg.EventsTableName
	EventsTableSchema     = iceberg.EventsTableSchema
	WriteManifest         = iceberg.WriteManifest
	WriteManifestList     = iceberg.WriteManifestList
	ReadManifest          = iceberg.ReadManifest
	ReadManifestList      = iceberg.ReadManifestList
	ExtractPartBucketKey  = iceberg.ExtractPartBucketKey
)
