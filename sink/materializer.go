package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	pq "github.com/parquet-go/parquet-go"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/metrics"
	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/pg2iceberg/pg2iceberg/worker"
)

// ChangeEventBuffer is a thread-safe buffer for change events, shared between
// the Sink (producer) and the Materializer (consumer). Created before both and
// injected into each, avoiding a circular dependency.
type ChangeEventBuffer struct {
	events map[string][]changeEvent
	mu     sync.Mutex
}

// NewChangeEventBuffer creates a new empty event buffer.
func NewChangeEventBuffer() *ChangeEventBuffer {
	return &ChangeEventBuffer{events: make(map[string][]changeEvent)}
}

// PushEvents adds change events for a table. Called by the Sink after flush.
func (b *ChangeEventBuffer) PushEvents(pgTable string, events []changeEvent) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events[pgTable] = append(b.events[pgTable], events...)
	metrics.MaterializerBufferSize.WithLabelValues(pgTable).Set(float64(len(b.events[pgTable])))
}

// Drain removes and returns all buffered events for a table.
func (b *ChangeEventBuffer) Drain(pgTable string) []changeEvent {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	events := b.events[pgTable]
	delete(b.events, pgTable)
	metrics.MaterializerBufferSize.WithLabelValues(pgTable).Set(0)
	return events
}

// fileIndex tracks which PKs live in which data files for a single table.
// Used to resolve TOAST unchanged columns by downloading only the affected files.
type fileIndex struct {
	// pkToFile maps PK key → file path (the DataFileInfo.Path).
	pkToFile map[string]string
	// files maps file path → DataFileInfo.
	files map[string]DataFileInfo
	// filePKs maps file path → set of PK keys in that file.
	filePKs map[string]map[string]bool
	// snapshotID is the materialized table snapshot this index was built from.
	snapshotID int64
}

func newFileIndex() *fileIndex {
	return &fileIndex{
		pkToFile: make(map[string]string),
		files:    make(map[string]DataFileInfo),
		filePKs:  make(map[string]map[string]bool),
	}
}

// addFile registers all PKs in a file.
func (fi *fileIndex) addFile(df DataFileInfo, pkKeys []string) {
	fi.files[df.Path] = df
	pks := make(map[string]bool, len(pkKeys))
	for _, pk := range pkKeys {
		fi.pkToFile[pk] = df.Path
		pks[pk] = true
	}
	fi.filePKs[df.Path] = pks
}

// affectedFiles returns the set of file paths that contain any of the given PKs.
func (fi *fileIndex) affectedFiles(pks []string) map[string]bool {
	paths := make(map[string]bool)
	for _, pk := range pks {
		if path, ok := fi.pkToFile[pk]; ok {
			paths[path] = true
		}
	}
	return paths
}

// Materializer reads change events from events tables and applies them to
// the materialized (flattened) tables using Merge-on-Read: equality delete
// files mark old rows, new data files contain updated rows. A file index
// tracks PK→file mappings to resolve TOAST unchanged columns when needed.
type Materializer struct {
	cfg     config.SinkConfig
	catalog Catalog
	s3      ObjectStorage
	tables  map[string]*tableSink
	buf     *ChangeEventBuffer

	// Tracks the last processed events snapshot per table.
	// Keyed by PG table name.
	lastEventsSnapshot map[string]int64

	// Per-table file index for TOAST resolution: tracks which PKs are in
	// which data files so we only download files containing affected PKs.
	fileIndexes map[string]*fileIndex
}

// NewMaterializer creates a new Materializer.
func NewMaterializer(cfg config.SinkConfig, catalog Catalog, s3 ObjectStorage, tables map[string]*tableSink, buf *ChangeEventBuffer) *Materializer {
	return &Materializer{
		cfg:                cfg,
		catalog:            catalog,
		s3:                 s3,
		tables:             tables,
		buf:                buf,
		lastEventsSnapshot: make(map[string]int64),
		fileIndexes:        make(map[string]*fileIndex),
	}
}

// SetLastEventsSnapshot restores the checkpoint for a table (called on startup).
func (m *Materializer) SetLastEventsSnapshot(pgTable string, snapshotID int64) {
	m.lastEventsSnapshot[pgTable] = snapshotID
}

// LastEventsSnapshots returns the current checkpoint state for persistence.
func (m *Materializer) LastEventsSnapshots() map[string]int64 {
	return m.lastEventsSnapshot
}

// MaterializeAll runs a single materialization pass for all tables.
// Called during graceful shutdown to ensure all flushed events are materialized.
func (m *Materializer) MaterializeAll(ctx context.Context) {
	for pgTable, ts := range m.tables {
		if err := m.MaterializeTable(ctx, pgTable, ts); err != nil {
			metrics.MaterializerErrorsTotal.WithLabelValues(pgTable).Inc()
			log.Printf("[materializer] final pass error for %s: %v", pgTable, err)
		}
	}
}

// Run starts the materialization loop. Blocks until ctx is cancelled.
func (m *Materializer) Run(ctx context.Context) {
	interval := m.cfg.MaterializerDuration()
	if interval <= 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for pgTable, ts := range m.tables {
				if err := m.MaterializeTable(ctx, pgTable, ts); err != nil {
					metrics.MaterializerErrorsTotal.WithLabelValues(pgTable).Inc()
					log.Printf("[materializer] error materializing %s: %v", pgTable, err)
				}
			}
		}
	}
}

// changeEvent represents a parsed change event from the events table.
type changeEvent struct {
	op            string // "I", "U", "D"
	lsn           int64
	seq           int64
	unchangedCols []string
	row           map[string]any // user columns only
}

// MaterializeTable reads new events for one table and applies them to the
// materialized table using Merge-on-Read. Instead of downloading and rewriting
// existing data files (CoW), it writes:
//   - Equality delete files: for UPDATEs and DELETEs (marks old rows for removal)
//   - New data files: for INSERTs and UPDATEs (contains the new/updated rows)
//
// Existing manifests from the previous snapshot are carried forward unchanged.
func (m *Materializer) MaterializeTable(ctx context.Context, pgTable string, ts *tableSink) error {
	start := time.Now()
	catalog := m.catalog
	s3 := m.s3
	ns := m.cfg.Namespace

	// Prefer in-memory events (fast path: no S3 reads).
	events := m.buf.Drain(pgTable)
	fromBuffer := len(events) > 0

	if !fromBuffer {
		// No in-memory events — fall back to S3 (crash recovery path).
		eventsTm, err := catalog.LoadTable(ns, ts.eventsIcebergName)
		if err != nil {
			return fmt.Errorf("load events table: %w", err)
		}
		if eventsTm == nil || eventsTm.Metadata.CurrentSnapshotID == 0 {
			return nil // no events yet
		}

		currentSnapshotID := eventsTm.Metadata.CurrentSnapshotID
		lastProcessed := m.lastEventsSnapshot[pgTable]

		if currentSnapshotID == lastProcessed {
			return nil // already up to date
		}

		newFiles, err := m.findNewEventFiles(ctx, s3, eventsTm, lastProcessed)
		if err != nil {
			return fmt.Errorf("find new event files: %w", err)
		}
		if len(newFiles) == 0 {
			m.lastEventsSnapshot[pgTable] = currentSnapshotID
			return nil
		}

		events, err = m.readEvents(ctx, s3, newFiles, ts.srcSchema)
		if err != nil {
			return fmt.Errorf("read events: %w", err)
		}
		if len(events) == 0 {
			m.lastEventsSnapshot[pgTable] = currentSnapshotID
			return nil
		}

		// Update checkpoint now — S3 events are loaded.
		m.lastEventsSnapshot[pgTable] = currentSnapshotID
	}

	// Sort events by _seq (globally monotonic counter) to ensure correct ordering.
	sort.Slice(events, func(i, j int) bool {
		return events[i].seq < events[j].seq
	})

	// Group events by PK — keep only the final state per PK.
	pk := ts.srcSchema.PK
	if len(pk) == 0 {
		// No PK — can't merge. Just skip materialization.
		log.Printf("[materializer] skipping %s: no primary key", pgTable)
		return nil
	}

	// Compute final state per PK from events by folding incrementally.
	// This ensures TOAST unchanged columns are resolved from the prior
	// accumulated state (e.g. INSERT → UPDATE chain within the same batch).
	type pkState struct {
		op            string
		row           map[string]any
		unchangedCols []string // only set when no prior state to resolve from
	}
	finalState := make(map[string]*pkState)

	for _, ev := range events {
		pkKey := buildPKKey(ev.row, pk)
		existing := finalState[pkKey]

		switch ev.op {
		case "I":
			finalState[pkKey] = &pkState{op: "I", row: ev.row}
		case "U":
			if existing != nil && len(ev.unchangedCols) > 0 {
				// Resolve unchanged columns from the accumulated state.
				merged := make(map[string]any, len(ev.row))
				for k, v := range ev.row {
					merged[k] = v
				}
				for _, col := range ev.unchangedCols {
					if val, ok := existing.row[col]; ok {
						merged[col] = val
					}
				}
				finalState[pkKey] = &pkState{op: "U", row: merged}
			} else {
				// No prior state in this batch — carry unchangedCols.
				// For MoR, unchanged cols remain nil in the new data file.
				// The equality delete removes the old row, and the new data
				// file has the complete row (minus TOAST cols that stay nil).
				finalState[pkKey] = &pkState{
					op:            "U",
					row:           ev.row,
					unchangedCols: ev.unchangedCols,
				}
			}
		case "D":
			finalState[pkKey] = &pkState{op: "D", row: ev.row}
		}
	}

	// Load the materialized table metadata.
	matTm, err := catalog.LoadTable(ns, ts.icebergName)
	if err != nil {
		return fmt.Errorf("load materialized table: %w", err)
	}

	// --- TOAST resolution via file index ---
	// Check if any UPDATEs have unresolved unchanged columns (TOAST).
	var unresolvedPKs []string
	for pkKey, state := range finalState {
		if state.op == "U" && len(state.unchangedCols) > 0 {
			unresolvedPKs = append(unresolvedPKs, pkKey)
		}
	}
	if len(unresolvedPKs) > 0 && matTm != nil && matTm.Metadata.CurrentSnapshotID > 0 {
		// Ensure we have a file index for this table.
		fi := m.fileIndexes[pgTable]
		if fi == nil || fi.snapshotID != matTm.Metadata.CurrentSnapshotID {
			fi, err = m.buildFileIndex(ctx, pgTable, ts, matTm)
			if err != nil {
				return fmt.Errorf("build file index: %w", err)
			}
			m.fileIndexes[pgTable] = fi
		}

		// Find which files contain the unresolved PKs.
		affectedFilePaths := fi.affectedFiles(unresolvedPKs)

		// Download affected files and resolve TOAST columns.
		resolved := 0
		for path := range affectedFilePaths {
			dfKey, err := KeyFromURI(path)
			if err != nil {
				continue
			}
			data, err := s3.Download(ctx, dfKey)
			if err != nil {
				log.Printf("[materializer] TOAST: failed to download %s: %v", path, err)
				continue
			}
			rows, err := readParquetRows(data, ts.srcSchema)
			if err != nil {
				log.Printf("[materializer] TOAST: failed to read %s: %v", path, err)
				continue
			}

			for _, row := range rows {
				pkKey := buildPKKey(row, pk)
				state, ok := finalState[pkKey]
				if !ok || state.op != "U" || len(state.unchangedCols) == 0 {
					continue
				}
				// Patch unchanged columns from the existing row.
				for _, col := range state.unchangedCols {
					if val, exists := row[col]; exists {
						state.row[col] = val
					}
				}
				state.unchangedCols = nil // mark as resolved
				resolved++
			}
		}
		if resolved > 0 {
			log.Printf("[materializer] TOAST: resolved %d/%d rows for %s (%d files scanned)",
				resolved, len(unresolvedPKs), pgTable, len(affectedFilePaths))
		}
	}

	var prevMatSnapID int64
	if matTm != nil {
		prevMatSnapID = matTm.Metadata.CurrentSnapshotID
	}
	seqNum := int64(1)
	if matTm != nil {
		seqNum = matTm.Metadata.LastSequenceNumber + 1
	}

	now := time.Now()
	snapshotID := now.UnixMilli()
	basePath := fmt.Sprintf("%s.db/%s", ns, ts.icebergName)
	targetSize := m.cfg.MaterializerTargetFileSizeOrDefault()
	pkFieldIDs := ts.srcSchema.PKFieldIDs()

	// --- Phase 1: Write equality delete files ---
	// For every PK where the final state is UPDATE or DELETE, write a delete
	// row containing only PK column values. These logically remove the old row
	// from existing data files at read time.
	var deleteEntries []ManifestEntry
	var deleteRowCount int64

	deleteWriter := NewRollingDeleteWriter(ts.srcSchema, targetSize)
	for _, state := range finalState {
		if state.op == "U" || state.op == "D" {
			// Delete writer only writes PK columns.
			if err := deleteWriter.Add(state.row); err != nil {
				return fmt.Errorf("add delete row: %w", err)
			}
			deleteRowCount++
		}
	}

	if deleteRowCount > 0 {
		chunks, err := deleteWriter.FlushAll()
		if err != nil {
			return fmt.Errorf("flush deletes: %w", err)
		}
		for i, chunk := range chunks {
			fileUUID := uuid.New().String()
			key := fmt.Sprintf("%s/data/%s-eq-delete-%d.parquet", basePath, fileUUID, i)
			uri, err := s3.Upload(ctx, key, chunk.Data)
			if err != nil {
				return fmt.Errorf("upload delete file: %w", err)
			}
			deleteEntries = append(deleteEntries, ManifestEntry{
				Status:     1, // added
				SnapshotID: snapshotID,
				DataFile: DataFileInfo{
					Path:             uri,
					FileSizeBytes:    int64(len(chunk.Data)),
					RecordCount:      chunk.RowCount,
					Content:          2, // equality deletes
					EqualityFieldIDs: pkFieldIDs,
				},
			})
		}
	}

	// --- Phase 2: Write new data files ---
	// For every PK where the final state is INSERT or UPDATE, write the
	// complete row to a new data file.
	var dataEntries []ManifestEntry

	if ts.partSpec == nil || ts.partSpec.IsUnpartitioned() {
		writer := NewRollingDataWriter(ts.srcSchema, targetSize)
		for _, state := range finalState {
			if state.op == "I" || state.op == "U" {
				if err := writer.Add(state.row); err != nil {
					return fmt.Errorf("add data row: %w", err)
				}
			}
		}
		chunks, err := writer.FlushAll()
		if err != nil {
			return fmt.Errorf("flush data: %w", err)
		}
		for i, chunk := range chunks {
			fileUUID := uuid.New().String()
			key := fmt.Sprintf("%s/data/%s-mat-%d.parquet", basePath, fileUUID, i)
			uri, err := s3.Upload(ctx, key, chunk.Data)
			if err != nil {
				return fmt.Errorf("upload data file: %w", err)
			}
			dataEntries = append(dataEntries, ManifestEntry{
				Status:     1,
				SnapshotID: snapshotID,
				DataFile: DataFileInfo{
					Path:          uri,
					FileSizeBytes: int64(len(chunk.Data)),
					RecordCount:   chunk.RowCount,
					Content:       0, // data
				},
			})
		}
	} else {
		// Partitioned: group rows by partition, write per-partition files.
		type partBucket struct {
			values map[string]any
			rows   []map[string]any
		}
		partitions := make(map[string]*partBucket)

		for _, state := range finalState {
			if state.op == "I" || state.op == "U" {
				pKey := ts.partSpec.PartitionKey(state.row, ts.srcSchema)
				pb, ok := partitions[pKey]
				if !ok {
					pb = &partBucket{
						values: ts.partSpec.PartitionValues(state.row, ts.srcSchema),
					}
					partitions[pKey] = pb
				}
				pb.rows = append(pb.rows, state.row)
			}
		}

		for _, pb := range partitions {
			writer := NewRollingDataWriter(ts.srcSchema, targetSize)
			for _, row := range pb.rows {
				if err := writer.Add(row); err != nil {
					return fmt.Errorf("add row: %w", err)
				}
			}
			chunks, err := writer.FlushAll()
			if err != nil {
				return fmt.Errorf("flush partition: %w", err)
			}

			partPath := ts.partSpec.PartitionPath(pb.values)
			avroPartValues := ts.partSpec.PartitionAvroValue(pb.values, ts.srcSchema)

			for i, chunk := range chunks {
				fileUUID := uuid.New().String()
				key := fmt.Sprintf("%s/data/%s/%s-mat-%d.parquet", basePath, partPath, fileUUID, i)
				uri, err := s3.Upload(ctx, key, chunk.Data)
				if err != nil {
					return fmt.Errorf("upload data file: %w", err)
				}
				dataEntries = append(dataEntries, ManifestEntry{
					Status:     1,
					SnapshotID: snapshotID,
					DataFile: DataFileInfo{
						Path:            uri,
						FileSizeBytes:   int64(len(chunk.Data)),
						RecordCount:     chunk.RowCount,
						Content:         0,
						PartitionValues: avroPartValues,
					},
				})
			}
		}
	}

	// Nothing to commit if no deletes and no data.
	if len(deleteEntries) == 0 && len(dataEntries) == 0 {
		if fromBuffer {
			eventsTm, err := catalog.LoadTable(ns, ts.eventsIcebergName)
			if err == nil && eventsTm != nil {
				m.lastEventsSnapshot[pgTable] = eventsTm.Metadata.CurrentSnapshotID
			}
		}
		return nil
	}

	// --- Phase 3: Carry forward existing manifests and commit ---
	// Load existing manifests from the previous snapshot's manifest list.
	// These are included unchanged in the new manifest list.
	var existingManifests []ManifestFileInfo
	if matTm != nil && matTm.Metadata.CurrentSnapshotID > 0 {
		var err error
		existingManifests, err = m.loadExistingManifests(ctx, s3, matTm)
		if err != nil {
			return fmt.Errorf("load existing manifests: %w", err)
		}
	}

	var newManifests []ManifestFileInfo

	// Write new data manifest (if we have data entries).
	if len(dataEntries) > 0 {
		manifestBytes, err := WriteManifest(ts.srcSchema, dataEntries, seqNum, 0, ts.partSpec)
		if err != nil {
			return fmt.Errorf("write data manifest: %w", err)
		}
		manifestKey := fmt.Sprintf("%s/metadata/%s-mat-data.avro", basePath, uuid.New().String())
		manifestURI, err := s3.Upload(ctx, manifestKey, manifestBytes)
		if err != nil {
			return fmt.Errorf("upload data manifest: %w", err)
		}
		var totalRows int64
		for _, e := range dataEntries {
			totalRows += e.DataFile.RecordCount
		}
		newManifests = append(newManifests, ManifestFileInfo{
			Path:           manifestURI,
			Length:         int64(len(manifestBytes)),
			Content:        0, // data
			SnapshotID:     snapshotID,
			AddedFiles:     len(dataEntries),
			AddedRows:      totalRows,
			SequenceNumber: seqNum,
		})
	}

	// Write new delete manifest (if we have delete entries).
	if len(deleteEntries) > 0 {
		manifestBytes, err := WriteManifest(ts.srcSchema, deleteEntries, seqNum, 1, ts.partSpec)
		if err != nil {
			return fmt.Errorf("write delete manifest: %w", err)
		}
		manifestKey := fmt.Sprintf("%s/metadata/%s-mat-deletes.avro", basePath, uuid.New().String())
		manifestURI, err := s3.Upload(ctx, manifestKey, manifestBytes)
		if err != nil {
			return fmt.Errorf("upload delete manifest: %w", err)
		}
		newManifests = append(newManifests, ManifestFileInfo{
			Path:           manifestURI,
			Length:         int64(len(manifestBytes)),
			Content:        1, // deletes
			SnapshotID:     snapshotID,
			AddedFiles:     len(deleteEntries),
			AddedRows:      deleteRowCount,
			SequenceNumber: seqNum,
		})
	}

	// Manifest list = existing manifests + new manifests.
	allManifests := append(existingManifests, newManifests...)

	mlBytes, err := WriteManifestList(allManifests)
	if err != nil {
		return fmt.Errorf("write manifest list: %w", err)
	}

	mlKey := fmt.Sprintf("%s/metadata/snap-%d-0-manifest-list.avro", basePath, snapshotID)
	mlURI, err := s3.Upload(ctx, mlKey, mlBytes)
	if err != nil {
		return fmt.Errorf("upload manifest list: %w", err)
	}

	commit := SnapshotCommit{
		SnapshotID:       snapshotID,
		SequenceNumber:   seqNum,
		TimestampMs:      now.UnixMilli(),
		ManifestListPath: mlURI,
		SchemaID:         ts.matSchemaID,
		Summary: map[string]string{
			"operation": "overwrite",
		},
	}

	if err := catalog.CommitSnapshot(ns, ts.icebergName, prevMatSnapID, commit); err != nil {
		return fmt.Errorf("commit materialized snapshot: %w", err)
	}

	// Invalidate the file index so it's rebuilt on next TOAST resolution.
	// MoR doesn't rewrite files, but equality deletes logically remove old rows.
	// Rebuilding ensures the index reflects the current state.
	if fi := m.fileIndexes[pgTable]; fi != nil {
		fi.snapshotID = 0 // force rebuild on next use
	}

	// Update checkpoint for in-memory path (S3 path already updated above).
	if fromBuffer {
		eventsTm, err := catalog.LoadTable(ns, ts.eventsIcebergName)
		if err == nil && eventsTm != nil {
			m.lastEventsSnapshot[pgTable] = eventsTm.Metadata.CurrentSnapshotID
		}
	}

	source := "buffer"
	if !fromBuffer {
		source = "s3"
	}
	duration := time.Since(start)

	// Track highest materialized LSN.
	var maxLSN int64
	for _, ev := range events {
		if ev.lsn > maxLSN {
			maxLSN = ev.lsn
		}
	}
	if maxLSN > 0 {
		metrics.MaterializerMaterializedLSN.WithLabelValues(pgTable).Set(float64(maxLSN))
	}

	metrics.MaterializerDurationSeconds.WithLabelValues(pgTable).Observe(duration.Seconds())
	metrics.MaterializerRunsTotal.WithLabelValues(pgTable, source).Inc()
	metrics.MaterializerEventsTotal.WithLabelValues(pgTable).Add(float64(len(events)))
	metrics.MaterializerDataFilesWrittenTotal.WithLabelValues(pgTable).Add(float64(len(dataEntries)))
	metrics.MaterializerDeleteFilesWrittenTotal.WithLabelValues(pgTable).Add(float64(len(deleteEntries)))
	metrics.MaterializerDeleteRowsTotal.WithLabelValues(pgTable).Add(float64(deleteRowCount))

	log.Printf("[materializer] materialized %s: %d events (%s), %d data files, %d delete files (%.1fs)",
		pgTable, len(events), source, len(dataEntries), len(deleteEntries), duration.Seconds())

	return nil
}

// loadExistingManifests reads all manifest file entries from the current
// snapshot's manifest list. These are carried forward unchanged into the
// new manifest list.
func (m *Materializer) loadExistingManifests(ctx context.Context, s3 ObjectStorage, tm *TableMetadata) ([]ManifestFileInfo, error) {
	mlURI := tm.CurrentManifestList()
	if mlURI == "" {
		return nil, nil
	}

	mlKey, err := KeyFromURI(mlURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest list URI: %w", err)
	}
	mlData, err := s3.Download(ctx, mlKey)
	if err != nil {
		return nil, fmt.Errorf("download manifest list: %w", err)
	}
	return ReadManifestList(mlData)
}

// buildFileIndex reads all data files for a materialized table and builds the
// PK→file index. Called on startup or when the index is missing (crash recovery).
func (m *Materializer) buildFileIndex(ctx context.Context, pgTable string, ts *tableSink, matTm *TableMetadata) (*fileIndex, error) {
	fi := newFileIndex()
	if matTm == nil || matTm.Metadata.CurrentSnapshotID == 0 {
		return fi, nil
	}
	fi.snapshotID = matTm.Metadata.CurrentSnapshotID

	allFiles, err := m.loadAllDataFiles(ctx, m.s3, matTm)
	if err != nil {
		return nil, fmt.Errorf("load data files for index: %w", err)
	}

	pk := ts.srcSchema.PK

	// Process files in parallel with bounded concurrency using the worker pool.
	maxConcurrency := m.cfg.MaterializerConcurrencyOrDefault()

	type fileResult struct {
		df     DataFileInfo
		pkKeys []string
	}

	var (
		mu          sync.Mutex
		fileResults []fileResult
	)

	tasks := make([]worker.Task, 0, len(allFiles))
	for _, df := range allFiles {
		dfKey, err := KeyFromURI(df.Path)
		if err != nil {
			continue
		}
		tasks = append(tasks, worker.Task{
			Name: dfKey,
			Fn: func(ctx context.Context, _ *worker.Progress) error {
				size, err := m.s3.StatObject(ctx, dfKey)
				if err != nil {
					return fmt.Errorf("stat %s: %w", df.Path, err)
				}
				ra := &s3ReaderAt{ctx: ctx, s3: m.s3, key: dfKey}
				pkKeys, err := readParquetPKKeysFromReaderAt(ra, size, pk)
				if err != nil {
					return fmt.Errorf("read PKs from %s: %w", df.Path, err)
				}
				mu.Lock()
				fileResults = append(fileResults, fileResult{df: df, pkKeys: pkKeys})
				mu.Unlock()
				return nil
			},
		})
	}

	pool := worker.NewPool(maxConcurrency)
	if _, err := pool.Run(ctx, tasks); err != nil {
		return nil, fmt.Errorf("build file index: %w", err)
	}

	for _, r := range fileResults {
		fi.addFile(r.df, r.pkKeys)
	}

	log.Printf("[materializer] built file index for %s: %d files, %d PKs", pgTable, len(fi.files), len(fi.pkToFile))
	return fi, nil
}

// loadAllDataFiles returns all live data files from a table's current snapshot.
func (m *Materializer) loadAllDataFiles(ctx context.Context, s3 ObjectStorage, tm *TableMetadata) ([]DataFileInfo, error) {
	mlURI := tm.CurrentManifestList()
	if mlURI == "" {
		return nil, nil
	}

	mlKey, err := KeyFromURI(mlURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest list URI: %w", err)
	}
	mlData, err := s3.Download(ctx, mlKey)
	if err != nil {
		return nil, fmt.Errorf("download manifest list: %w", err)
	}
	manifestInfos, err := ReadManifestList(mlData)
	if err != nil {
		return nil, fmt.Errorf("read manifest list: %w", err)
	}

	var dataFiles []DataFileInfo
	for _, mfi := range manifestInfos {
		if mfi.Content != 0 {
			continue
		}
		mKey, err := KeyFromURI(mfi.Path)
		if err != nil {
			continue
		}
		mData, err := s3.Download(ctx, mKey)
		if err != nil {
			continue
		}
		entries, err := ReadManifest(mData)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.Status == 2 || e.DataFile.Content != 0 {
				continue
			}
			dataFiles = append(dataFiles, e.DataFile)
		}
	}
	return dataFiles, nil
}

// findNewEventFiles returns data files added to the events table since the
// given snapshot ID. If lastSnapshotID is 0, returns all data files.
func (m *Materializer) findNewEventFiles(ctx context.Context, s3 ObjectStorage, tm *TableMetadata, lastSnapshotID int64) ([]DataFileInfo, error) {
	mlURI := tm.CurrentManifestList()
	if mlURI == "" {
		return nil, nil
	}

	mlKey, err := KeyFromURI(mlURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest list URI: %w", err)
	}
	mlData, err := s3.Download(ctx, mlKey)
	if err != nil {
		return nil, fmt.Errorf("download manifest list: %w", err)
	}
	manifestInfos, err := ReadManifestList(mlData)
	if err != nil {
		return nil, fmt.Errorf("read manifest list: %w", err)
	}

	var dataFiles []DataFileInfo
	for _, mfi := range manifestInfos {
		if mfi.Content != 0 {
			continue
		}
		// If we have a checkpoint, only read manifests added after it.
		if lastSnapshotID > 0 && mfi.SnapshotID <= lastSnapshotID {
			continue
		}

		mKey, err := KeyFromURI(mfi.Path)
		if err != nil {
			continue
		}
		mData, err := s3.Download(ctx, mKey)
		if err != nil {
			continue
		}
		entries, err := ReadManifest(mData)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.Status == 2 || e.DataFile.Content != 0 {
				continue
			}
			dataFiles = append(dataFiles, e.DataFile)
		}
	}
	return dataFiles, nil
}

// readEvents reads event parquet files and parses them into changeEvent structs.
func (m *Materializer) readEvents(ctx context.Context, s3 ObjectStorage, files []DataFileInfo, srcSchema *schema.TableSchema) ([]changeEvent, error) {
	var events []changeEvent

	// Build the events schema to read the parquet files.
	eventsSchema := EventsTableSchema(srcSchema)

	for _, df := range files {
		key, err := KeyFromURI(df.Path)
		if err != nil {
			continue
		}
		data, err := s3.Download(ctx, key)
		if err != nil {
			log.Printf("[materializer] failed to download event file %s: %v", df.Path, err)
			continue
		}

		rows, err := readParquetRows(data, eventsSchema)
		if err != nil {
			log.Printf("[materializer] failed to read event file %s: %v", df.Path, err)
			continue
		}

		for _, row := range rows {
			ev := changeEvent{
				op:  fmt.Sprintf("%v", row["_op"]),
				lsn: toInt64(row["_lsn"]),
				seq: toInt64(row["_seq"]),
			}

			// Parse unchanged cols.
			if uc, ok := row["_unchanged_cols"]; ok && uc != nil {
				ucStr := fmt.Sprintf("%v", uc)
				if ucStr != "" {
					ev.unchangedCols = strings.Split(ucStr, ",")
				}
			}

			// Extract user columns only.
			ev.row = make(map[string]any, len(srcSchema.Columns))
			for _, col := range srcSchema.Columns {
				ev.row[col.Name] = row[col.Name]
			}

			events = append(events, ev)
		}
	}

	return events, nil
}

// readParquetRows reads a parquet file and returns rows as maps.
func readParquetRows(data []byte, ts *schema.TableSchema) ([]map[string]any, error) {
	reader := pq.NewReader(bytes.NewReader(data))

	colNames := make([]string, len(reader.Schema().Fields()))
	for i, f := range reader.Schema().Fields() {
		colNames[i] = f.Name()
	}

	var rows []map[string]any
	rowBuf := make([]pq.Row, 1)
	for {
		n, err := reader.ReadRows(rowBuf)
		if n == 0 {
			break
		}
		if err != nil && n == 0 {
			return nil, fmt.Errorf("read row: %w", err)
		}

		row := make(map[string]any, len(colNames))
		for _, v := range rowBuf[0] {
			colIdx := v.Column()
			if colIdx >= 0 && colIdx < len(colNames) {
				if v.IsNull() {
					row[colNames[colIdx]] = nil
				} else {
					row[colNames[colIdx]] = parquetValueToGo(v)
				}
			}
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// parquetValueToGo converts a parquet.Value to a Go native type.
func parquetValueToGo(v pq.Value) any {
	switch v.Kind() {
	case pq.Boolean:
		return v.Boolean()
	case pq.Int32:
		return v.Int32()
	case pq.Int64:
		return v.Int64()
	case pq.Float:
		return v.Float()
	case pq.Double:
		return v.Double()
	case pq.ByteArray, pq.FixedLenByteArray:
		return string(v.ByteArray())
	default:
		return v.String()
	}
}

// readParquetPKKeysFromReaderAt reads only PK columns from a parquet file via
// an io.ReaderAt (backed by S3 range reads). Only the footer and PK column
// chunks are fetched — non-PK column data is never read from storage.
func readParquetPKKeysFromReaderAt(r io.ReaderAt, size int64, pk []string) ([]string, error) {
	file, err := pq.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	schema := file.Schema()

	// Find PK column indices in the parquet schema.
	pkColIdx := make(map[string]int, len(pk))
	for i, f := range schema.Fields() {
		for _, p := range pk {
			if f.Name() == p {
				pkColIdx[p] = i
			}
		}
	}
	if len(pkColIdx) != len(pk) {
		return nil, fmt.Errorf("parquet schema missing PK columns: have %v, want %v", pkColIdx, pk)
	}

	var keys []string

	for _, rg := range file.RowGroups() {
		numRows := rg.NumRows()

		// Read values from each PK column chunk.
		pkValues := make(map[string][]string, len(pk))
		for _, p := range pk {
			idx := pkColIdx[p]
			chunk := rg.ColumnChunks()[idx]
			pages := chunk.Pages()

			vals := make([]string, 0, numRows)
			for {
				page, err := pages.ReadPage()
				if page == nil || err != nil {
					break
				}
				pageValues := page.Values()
				vBuf := make([]pq.Value, page.NumValues())
				n, _ := pageValues.ReadValues(vBuf)
				for i := 0; i < n; i++ {
					if vBuf[i].IsNull() {
						vals = append(vals, "<nil>")
					} else {
						vals = append(vals, fmt.Sprintf("%v", parquetValueToGo(vBuf[i])))
					}
				}
			}
			pages.Close()
			pkValues[p] = vals
		}

		// Build PK key strings.
		nRows := int(numRows)
		if len(pk) == 1 {
			keys = append(keys, pkValues[pk[0]]...)
		} else {
			for i := 0; i < nRows; i++ {
				var b strings.Builder
				for j, col := range pk {
					if j > 0 {
						b.WriteByte(0)
					}
					if i < len(pkValues[col]) {
						b.WriteString(pkValues[col][i])
					}
				}
				keys = append(keys, b.String())
			}
		}
	}

	return keys, nil
}

// buildPKKey creates a unique key for a row based on its PK columns.
// Uses null byte as separator to avoid collisions.
func buildPKKey(row map[string]any, pk []string) string {
	if len(pk) == 1 {
		return fmt.Sprintf("%v", row[pk[0]])
	}
	var b strings.Builder
	for i, col := range pk {
		if i > 0 {
			b.WriteByte(0)
		}
		fmt.Fprintf(&b, "%v", row[col])
	}
	return b.String()
}
