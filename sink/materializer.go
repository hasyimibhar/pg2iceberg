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
	return events
}

// fileIndex tracks which PKs live in which data files for a single table.
// Built by the materializer when it writes files, and rebuilt from S3 on startup.
type fileIndex struct {
	// pkToFile maps PK key → file path (the DataFileInfo.Path).
	pkToFile map[string]string
	// files maps file path → DataFileInfo (for carry-forward manifest entries).
	files map[string]DataFileInfo
	// filePKs maps file path → set of PK keys in that file (for rewrite).
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

// removeFile removes a file and its PKs from the index.
func (fi *fileIndex) removeFile(path string) {
	for pk := range fi.filePKs[path] {
		delete(fi.pkToFile, pk)
	}
	delete(fi.files, path)
	delete(fi.filePKs, path)
}

// affectedFiles returns the set of file paths that contain any of the given PKs.
func (fi *fileIndex) affectedFiles(pks map[string]bool) map[string]bool {
	paths := make(map[string]bool)
	for pk := range pks {
		if path, ok := fi.pkToFile[pk]; ok {
			paths[path] = true
		}
	}
	return paths
}

// Materializer reads change events from events tables and upserts them into
// the materialized (flattened) tables. It runs on a configurable interval.
type Materializer struct {
	cfg     config.SinkConfig
	catalog Catalog
	s3      ObjectStorage
	tables  map[string]*tableSink
	buf     *ChangeEventBuffer

	// Tracks the last processed events snapshot per table.
	// Keyed by PG table name.
	lastEventsSnapshot map[string]int64

	// Per-table file index for incremental CoW: tracks which PKs are in
	// which data files so we only download affected files.
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
		mu         sync.Mutex
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
					log.Printf("[materializer] error materializing %s: %v", pgTable, err)
				}
			}
		}
	}
}

// changeEvent represents a parsed change event from the events table.
type changeEvent struct {
	op             string // "I", "U", "D"
	lsn            int64
	seq            int64
	unchangedCols  []string
	row            map[string]any // user columns only
}

// MaterializeTable reads new events for one table and merges into the flattened table.
// It prefers in-memory events pushed by the sink after flush. Falls back to reading
// from S3 when the buffer is empty (crash recovery on startup).
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
	// NOTE: _lsn (= walEnd = walStart + len(walData)) is NOT a valid ordering key
	// because walEnd varies with payload size — a large INSERT can have a higher
	// walEnd than subsequent UPDATEs, breaking chronological order.
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
				// No prior state in this batch — carry unchangedCols for
				// resolution against the materialized table later.
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

	// Ensure we have a file index for this table. On first run or after crash,
	// this scans all files once. Subsequent ticks use the maintained index.
	fi := m.fileIndexes[pgTable]
	if fi == nil || (matTm != nil && fi.snapshotID != matTm.Metadata.CurrentSnapshotID) {
		fi, err = m.buildFileIndex(ctx, pgTable, ts, matTm)
		if err != nil {
			return fmt.Errorf("build file index: %w", err)
		}
		m.fileIndexes[pgTable] = fi
	}

	// Build the set of affected PKs from events.
	affectedPKs := make(map[string]bool, len(finalState))
	for pkKey := range finalState {
		affectedPKs[pkKey] = true
	}

	// Incremental CoW: use the file index to find which files contain affected
	// PKs. Only download those files. Untouched files are carried forward.
	affectedFilePaths := fi.affectedFiles(affectedPKs)

	var carryForward []ManifestEntry
	affectedRows := make(map[string]map[string]any)
	var rewriteRows []map[string]any

	var matSnapID int64
	if matTm != nil {
		matSnapID = matTm.Metadata.CurrentSnapshotID
	}

	for path, df := range fi.files {
		if !affectedFilePaths[path] {
			// File has no affected PKs — carry forward unchanged.
			carryForward = append(carryForward, ManifestEntry{
				Status:     0, // existing
				SnapshotID: matSnapID,
				DataFile:   df,
			})
			continue
		}

		// File contains affected PKs — download and split rows.
		dfKey, err := KeyFromURI(df.Path)
		if err != nil {
			continue
		}
		data, err := s3.Download(ctx, dfKey)
		if err != nil {
			return fmt.Errorf("download affected file %s: %w", df.Path, err)
		}
		rows, err := readParquetRows(data, ts.srcSchema)
		if err != nil {
			return fmt.Errorf("read affected file %s: %w", df.Path, err)
		}

		for _, row := range rows {
			pkKey := buildPKKey(row, pk)
			if affectedPKs[pkKey] {
				affectedRows[pkKey] = row
			} else {
				rewriteRows = append(rewriteRows, row)
			}
		}
	}

	// Merge: apply events to affected rows.
	mergedRows := make(map[string]map[string]any, len(finalState))
	for pkKey, state := range finalState {
		switch state.op {
		case "I":
			mergedRows[pkKey] = state.row
		case "U":
			existing := affectedRows[pkKey]
			if existing == nil {
				mergedRows[pkKey] = state.row
			} else {
				merged := make(map[string]any, len(state.row))
				for k, v := range state.row {
					merged[k] = v
				}
				for _, col := range state.unchangedCols {
					if val, ok := existing[col]; ok {
						merged[col] = val
					}
				}
				mergedRows[pkKey] = merged
			}
		case "D":
			// Don't add to mergedRows — the row is removed.
		}
	}

	// Combine: unaffected rows from rewritten files + merged rows = new files.
	var outputRows []map[string]any
	outputRows = append(outputRows, rewriteRows...)
	for _, row := range mergedRows {
		outputRows = append(outputRows, row)
	}

	// Write new data files (only for the rewritten portion).
	targetSize := m.cfg.CompactionTargetSizeOrDefault()
	now := time.Now()
	snapshotID := now.UnixMilli()
	basePath := fmt.Sprintf("%s.db/%s", ns, ts.icebergName)

	var prevMatSnapID int64
	if matTm != nil {
		prevMatSnapID = matTm.Metadata.CurrentSnapshotID
	}
	seqNum := int64(1)
	if matTm != nil {
		seqNum = matTm.Metadata.LastSequenceNumber + 1
	}

	// newFilePKs tracks PK keys per new file for updating the file index.
	type newFileInfo struct {
		entry ManifestEntry
		pkKeys []string
	}
	var newFiles []newFileInfo

	if len(outputRows) > 0 {
		// Build parallel PK list in write order.
		outputPKs := make([]string, len(outputRows))
		for i, row := range outputRows {
			outputPKs[i] = buildPKKey(row, pk)
		}

		if ts.partSpec == nil || ts.partSpec.IsUnpartitioned() {
			writer := NewRollingDataWriter(ts.srcSchema, targetSize)
			for _, row := range outputRows {
				if err := writer.Add(row); err != nil {
					return fmt.Errorf("add row: %w", err)
				}
			}
			chunks, err := writer.FlushAll()
			if err != nil {
				return fmt.Errorf("flush materialized: %w", err)
			}
			pkOffset := 0
			for i, chunk := range chunks {
				fileUUID := uuid.New().String()
				key := fmt.Sprintf("%s/data/%s-mat-%d.parquet", basePath, fileUUID, i)
				uri, err := s3.Upload(ctx, key, chunk.Data)
				if err != nil {
					return fmt.Errorf("upload materialized file: %w", err)
				}
				newFiles = append(newFiles, newFileInfo{
					entry: ManifestEntry{
						Status:     1,
						SnapshotID: snapshotID,
						DataFile: DataFileInfo{
							Path:          uri,
							FileSizeBytes: int64(len(chunk.Data)),
							RecordCount:   chunk.RowCount,
							Content:       0,
						},
					},
					pkKeys: outputPKs[pkOffset : pkOffset+int(chunk.RowCount)],
				})
				pkOffset += int(chunk.RowCount)
			}
		} else {
			type partBucket struct {
				key    string
				values map[string]any
				rows   []map[string]any
				pkKeys []string
			}
			partitions := make(map[string]*partBucket)

			for i, row := range outputRows {
				pKey := ts.partSpec.PartitionKey(row, ts.srcSchema)
				pb, ok := partitions[pKey]
				if !ok {
					pb = &partBucket{
						key:    pKey,
						values: ts.partSpec.PartitionValues(row, ts.srcSchema),
					}
					partitions[pKey] = pb
				}
				pb.rows = append(pb.rows, row)
				pb.pkKeys = append(pb.pkKeys, outputPKs[i])
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
				pkOffset := 0

				for i, chunk := range chunks {
					fileUUID := uuid.New().String()
					key := fmt.Sprintf("%s/data/%s/%s-mat-%d.parquet", basePath, partPath, fileUUID, i)
					uri, err := s3.Upload(ctx, key, chunk.Data)
					if err != nil {
						return fmt.Errorf("upload materialized file: %w", err)
					}
					newFiles = append(newFiles, newFileInfo{
						entry: ManifestEntry{
							Status:     1,
							SnapshotID: snapshotID,
							DataFile: DataFileInfo{
								Path:            uri,
								FileSizeBytes:   int64(len(chunk.Data)),
								RecordCount:     chunk.RowCount,
								Content:         0,
								PartitionValues: avroPartValues,
							},
						},
						pkKeys: pb.pkKeys[pkOffset : pkOffset+int(chunk.RowCount)],
					})
					pkOffset += int(chunk.RowCount)
				}
			}
		}
	}

	// Collect manifest entries.
	newEntries := make([]ManifestEntry, len(newFiles))
	for i, nf := range newFiles {
		newEntries[i] = nf.entry
	}

	// Build manifest: carried-forward entries + new entries.
	allEntries := append(carryForward, newEntries...)

	// Handle empty table (all rows deleted).
	if len(allEntries) == 0 && prevMatSnapID == 0 {
		// Nothing to commit — no existing table, no new rows.
		if fromBuffer {
			eventsTm, err := catalog.LoadTable(ns, ts.eventsIcebergName)
			if err == nil && eventsTm != nil {
				m.lastEventsSnapshot[pgTable] = eventsTm.Metadata.CurrentSnapshotID
			}
		}
		return nil
	}

	var manifestInfos []ManifestFileInfo
	if len(allEntries) > 0 {
		manifestBytes, err := WriteManifest(ts.srcSchema, allEntries, seqNum, 0, ts.partSpec)
		if err != nil {
			return fmt.Errorf("write materialized manifest: %w", err)
		}

		manifestKey := fmt.Sprintf("%s/metadata/%s-mat-m0.avro", basePath, uuid.New().String())
		manifestURI, err := s3.Upload(ctx, manifestKey, manifestBytes)
		if err != nil {
			return fmt.Errorf("upload materialized manifest: %w", err)
		}

		var totalRows int64
		for _, e := range allEntries {
			totalRows += e.DataFile.RecordCount
		}
		manifestInfos = append(manifestInfos, ManifestFileInfo{
			Path:           manifestURI,
			Length:         int64(len(manifestBytes)),
			Content:        0,
			SnapshotID:     snapshotID,
			AddedFiles:     len(newEntries),
			AddedRows:      totalRows,
			SequenceNumber: seqNum,
		})
	}

	mlBytes, err := WriteManifestList(manifestInfos)
	if err != nil {
		return fmt.Errorf("write materialized manifest list: %w", err)
	}

	mlKey := fmt.Sprintf("%s/metadata/snap-%d-0-manifest-list.avro", basePath, snapshotID)
	mlURI, err := s3.Upload(ctx, mlKey, mlBytes)
	if err != nil {
		return fmt.Errorf("upload materialized manifest list: %w", err)
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

	// Update file index: remove rewritten files, add new files.
	for path := range affectedFilePaths {
		fi.removeFile(path)
	}
	for _, nf := range newFiles {
		fi.addFile(nf.entry.DataFile, nf.pkKeys)
	}
	fi.snapshotID = snapshotID

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
	log.Printf("[materializer] materialized %s: %d events (%s), %d carried, %d rewritten, %d new files (%.1fs)",
		pgTable, len(events), source, len(carryForward), len(rewriteRows), len(newEntries), duration.Seconds())

	return nil
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
// Returns native Go types (int32, int64, float32, float64, bool, string)
// so the writer's type conversion functions handle them correctly on roundtrip.
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

// extractPKString creates a string key from PK columns for set membership testing.
func extractPKString(row map[string]any, pk []string) string {
	if len(pk) == 1 {
		return fmt.Sprintf("%v", row[pk[0]])
	}
	key := ""
	for i, col := range pk {
		if i > 0 {
			key += "|"
		}
		key += fmt.Sprintf("%v", row[col])
	}
	return key
}
