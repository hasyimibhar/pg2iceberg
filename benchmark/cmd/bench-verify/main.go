package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/linkedin/goavro/v2"
	pq "github.com/parquet-go/parquet-go"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	cfg := loadConfig()
	log.Printf("bench-verify: namespace=%s table=%s", cfg.Namespace, cfg.Table)

	// Step 1: Replay operations log from PG to compute expected state.
	log.Println("step 1: loading operations log from postgres...")
	expected, err := loadExpectedState(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("load expected state: %v", err)
	}
	log.Printf("  expected: %d live rows", len(expected))

	// Step 2: Read all data from Iceberg (Parquet files in S3).
	log.Println("step 2: reading iceberg table from S3...")
	actual, err := loadIcebergState(ctx, cfg)
	if err != nil {
		log.Fatalf("load iceberg state: %v", err)
	}
	log.Printf("  iceberg: %d rows read", len(actual))

	// Step 3: Compare.
	log.Println("step 3: verifying correctness...")
	result := verify(expected, actual)
	printResult(result)

	if !result.Pass {
		os.Exit(1)
	}
}

// --- Config ---

type config struct {
	DatabaseURL string
	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3Region    string
	CatalogURI  string
	Warehouse   string
	Namespace   string
	Table       string
}

func loadConfig() config {
	return config{
		DatabaseURL: requireEnv("DATABASE_URL"),
		S3Endpoint:  requireEnv("S3_ENDPOINT"),
		S3AccessKey: requireEnv("S3_ACCESS_KEY"),
		S3SecretKey: requireEnv("S3_SECRET_KEY"),
		S3Region:    envOr("S3_REGION", "us-east-1"),
		CatalogURI:  requireEnv("CATALOG_URI"),
		Warehouse:   requireEnv("WAREHOUSE"),
		Namespace:   requireEnv("NAMESPACE"),
		Table:       requireEnv("TABLE"),
	}
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// --- Expected State ---

// expectedRow represents what we expect a row to look like after replaying all operations.
type expectedRow struct {
	Value int64
}

// loadExpectedState computes the expected final state.
// It first checks for an operations log (used by stream mode). If empty,
// it falls back to reading bench_events directly (seed-only mode where
// every row is an insert with value = seq).
func loadExpectedState(ctx context.Context, dsn string) (map[int64]expectedRow, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	// Check if operations log has entries.
	var opsCount int64
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM bench_operations").Scan(&opsCount); err != nil {
		return nil, fmt.Errorf("count operations: %w", err)
	}

	if opsCount > 0 {
		return loadExpectedFromOpsLog(ctx, pool)
	}

	// No operations log — seed-only mode. Read directly from bench_events.
	log.Println("  no operations log found, reading expected state from bench_events (seed-only mode)")
	return loadExpectedFromTable(ctx, pool)
}

func loadExpectedFromOpsLog(ctx context.Context, pool *pgxpool.Pool) (map[int64]expectedRow, error) {
	rows, err := pool.Query(ctx, "SELECT op, seq, value FROM bench_operations ORDER BY id ASC")
	if err != nil {
		return nil, fmt.Errorf("query operations: %w", err)
	}
	defer rows.Close()

	state := make(map[int64]expectedRow)
	for rows.Next() {
		var op string
		var seq int64
		var value *int64
		if err := rows.Scan(&op, &seq, &value); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		switch op {
		case "insert":
			if value != nil {
				state[seq] = expectedRow{Value: *value}
			}
		case "update":
			if _, ok := state[seq]; ok && value != nil {
				state[seq] = expectedRow{Value: *value}
			}
		case "delete":
			delete(state, seq)
		}
	}
	return state, rows.Err()
}

func loadExpectedFromTable(ctx context.Context, pool *pgxpool.Pool) (map[int64]expectedRow, error) {
	var totalRows int64
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM bench_events").Scan(&totalRows); err != nil {
		return nil, fmt.Errorf("count bench_events: %w", err)
	}
	log.Printf("  bench_events has %d rows, loading...", totalRows)

	rows, err := pool.Query(ctx, "SELECT seq, value FROM bench_events")
	if err != nil {
		return nil, fmt.Errorf("query bench_events: %w", err)
	}
	defer rows.Close()

	state := make(map[int64]expectedRow, totalRows)
	loaded := int64(0)
	for rows.Next() {
		var seq int64
		var value int64
		if err := rows.Scan(&seq, &value); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		state[seq] = expectedRow{Value: value}
		loaded++
		if loaded%1000000 == 0 {
			log.Printf("  loaded %dM / %dM rows from bench_events", loaded/1000000, totalRows/1000000)
		}
	}
	log.Printf("  loaded %d rows from bench_events", loaded)
	return state, rows.Err()
}

func countOps(state map[int64]expectedRow) int {
	// This is just the number of live rows; the actual op count isn't needed.
	return len(state)
}

// --- Iceberg State ---

// icebergRow is a row as read from Parquet data files.
type icebergRow struct {
	Value int64
}

func loadIcebergState(ctx context.Context, cfg config) (map[int64]icebergRow, error) {
	s3Client := newS3Client(cfg)

	// Load table metadata from catalog.
	manifestListURI, err := getManifestListURI(cfg)
	if err != nil {
		return nil, fmt.Errorf("get manifest list: %w", err)
	}
	if manifestListURI == "" {
		return nil, fmt.Errorf("no snapshots found for table %s.%s", cfg.Namespace, cfg.Table)
	}

	// Download and parse manifest list.
	mlKey := s3KeyFromURI(manifestListURI)
	mlData, err := s3Download(ctx, s3Client, cfg.Warehouse, mlKey)
	if err != nil {
		return nil, fmt.Errorf("download manifest list: %w", err)
	}

	manifestInfos, err := readManifestList(mlData)
	if err != nil {
		return nil, fmt.Errorf("parse manifest list: %w", err)
	}

	// Collect all data files and delete files from manifests.
	var dataFilePaths []string
	var deleteFilePaths []string

	for _, mfi := range manifestInfos {
		mKey := s3KeyFromURI(mfi.Path)
		mData, err := s3Download(ctx, s3Client, cfg.Warehouse, mKey)
		if err != nil {
			log.Printf("  warning: failed to download manifest %s: %v", mfi.Path, err)
			continue
		}
		entries, err := readManifest(mData)
		if err != nil {
			log.Printf("  warning: failed to parse manifest %s: %v", mfi.Path, err)
			continue
		}
		for _, e := range entries {
			if e.Status == 2 { // deleted
				continue
			}
			switch e.Content {
			case 0: // data
				dataFilePaths = append(dataFilePaths, e.FilePath)
			case 2: // equality delete
				deleteFilePaths = append(deleteFilePaths, e.FilePath)
			}
		}
	}

	log.Printf("  found %d data files, %d delete files", len(dataFilePaths), len(deleteFilePaths))

	// Build delete set from equality delete files.
	deleteSet := make(map[int64]struct{})
	for _, path := range deleteFilePaths {
		key := s3KeyFromURI(path)
		data, err := s3Download(ctx, s3Client, cfg.Warehouse, key)
		if err != nil {
			log.Printf("  warning: failed to download delete file %s: %v", path, err)
			continue
		}
		delRows, err := readParquetRows(data)
		if err != nil {
			log.Printf("  warning: failed to read delete file %s: %v", path, err)
			continue
		}
		for _, row := range delRows {
			if id, ok := row["id"]; ok {
				deleteSet[toInt64(id)] = struct{}{}
			}
		}
	}
	log.Printf("  delete set: %d entries", len(deleteSet))

	// Read all data files, apply deletes, deduplicate by id (keep last).
	result := make(map[int64]icebergRow)
	totalRead := 0
	for _, path := range dataFilePaths {
		key := s3KeyFromURI(path)
		data, err := s3Download(ctx, s3Client, cfg.Warehouse, key)
		if err != nil {
			log.Printf("  warning: failed to download data file %s: %v", path, err)
			continue
		}
		rows, err := readParquetRows(data)
		if err != nil {
			log.Printf("  warning: failed to read data file %s: %v", path, err)
			continue
		}

		for _, row := range rows {
			totalRead++
			id := toInt64(row["id"])

			// Skip deleted rows.
			if _, deleted := deleteSet[id]; deleted {
				continue
			}

			seq := toInt64(row["seq"])
			value := toInt64(row["value"])
			// For duplicate IDs (from updates), keep the latest by overwriting.
			result[seq] = icebergRow{Value: value}
		}
	}
	log.Printf("  total parquet rows read: %d, after dedup+delete: %d", totalRead, len(result))

	return result, nil
}

// --- Verification ---

type verifyResult struct {
	Pass           bool
	TotalExpected  int
	TotalActual    int
	Missing        []int64 // seqs in expected but not in actual
	Extra          []int64 // seqs in actual but not in expected
	WrongValue     []int64 // seqs present in both but with wrong value
	MissingSample  int     // how many missing seqs to print
	ExtraSample    int
	WrongSample    int
}

func verify(expected map[int64]expectedRow, actual map[int64]icebergRow) verifyResult {
	r := verifyResult{
		TotalExpected: len(expected),
		TotalActual:   len(actual),
		MissingSample: 10,
		ExtraSample:   10,
		WrongSample:   10,
	}

	// Check for missing rows (in expected, not in actual).
	for seq := range expected {
		if _, ok := actual[seq]; !ok {
			r.Missing = append(r.Missing, seq)
		}
	}

	// Check for extra rows (in actual, not in expected).
	for seq := range actual {
		if _, ok := expected[seq]; !ok {
			r.Extra = append(r.Extra, seq)
		}
	}

	// Check for wrong values.
	for seq, exp := range expected {
		if act, ok := actual[seq]; ok {
			if exp.Value != act.Value {
				r.WrongValue = append(r.WrongValue, seq)
			}
		}
	}

	r.Pass = len(r.Missing) == 0 && len(r.Extra) == 0 && len(r.WrongValue) == 0
	return r
}

func printResult(r verifyResult) {
	log.Println("========== VERIFICATION RESULT ==========")
	if r.Pass {
		log.Printf("PASS: all %d expected rows found with correct values", r.TotalExpected)
	} else {
		log.Println("FAIL")
	}
	log.Printf("  expected rows:    %d", r.TotalExpected)
	log.Printf("  actual rows:      %d", r.TotalActual)
	log.Printf("  missing rows:     %d", len(r.Missing))
	log.Printf("  extra rows:       %d", len(r.Extra))
	log.Printf("  wrong value rows: %d", len(r.WrongValue))

	if len(r.Missing) > 0 {
		n := r.MissingSample
		if n > len(r.Missing) {
			n = len(r.Missing)
		}
		log.Printf("  missing sample (first %d): %v", n, r.Missing[:n])
	}
	if len(r.Extra) > 0 {
		n := r.ExtraSample
		if n > len(r.Extra) {
			n = len(r.Extra)
		}
		log.Printf("  extra sample (first %d): %v", n, r.Extra[:n])
	}
	if len(r.WrongValue) > 0 {
		n := r.WrongSample
		if n > len(r.WrongValue) {
			n = len(r.WrongValue)
		}
		log.Printf("  wrong value sample (first %d): %v", n, r.WrongValue[:n])
	}
	log.Println("==========================================")
}

// --- S3 helpers ---

func newS3Client(cfg config) *s3.Client {
	return s3.New(s3.Options{
		Region:       cfg.S3Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, ""),
		BaseEndpoint: &cfg.S3Endpoint,
		UsePathStyle: true,
	})
}

func s3Download(ctx context.Context, client *s3.Client, warehouse, key string) ([]byte, error) {
	u, _ := url.Parse(warehouse)
	bucket := u.Host
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func s3KeyFromURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return uri
	}
	if len(u.Path) > 1 {
		return u.Path[1:]
	}
	return uri
}

// --- Iceberg catalog helpers ---

func getManifestListURI(cfg config) (string, error) {
	url := fmt.Sprintf("%s/v1/namespaces/%s/tables/%s", strings.TrimRight(cfg.CatalogURI, "/"), cfg.Namespace, cfg.Table)
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return "", fmt.Errorf("table not found: %s.%s", cfg.Namespace, cfg.Table)
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("catalog error %d: %s", resp.StatusCode, string(body))
	}

	var tm struct {
		Metadata struct {
			CurrentSnapshotID int64 `json:"current-snapshot-id"`
			Snapshots         []struct {
				SnapshotID   int64  `json:"snapshot-id"`
				ManifestList string `json:"manifest-list"`
			} `json:"snapshots"`
		} `json:"metadata"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tm); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}

	for _, snap := range tm.Metadata.Snapshots {
		if snap.SnapshotID == tm.Metadata.CurrentSnapshotID {
			return snap.ManifestList, nil
		}
	}
	return "", nil
}

// --- Avro manifest reading ---

type manifestInfo struct {
	Path    string
	Content int
}

type manifestEntry struct {
	Status   int
	Content  int
	FilePath string
}

func readManifestList(data []byte) ([]manifestInfo, error) {
	reader, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	var infos []manifestInfo
	for reader.Scan() {
		record, err := reader.Read()
		if err != nil {
			return nil, err
		}
		m, ok := record.(map[string]any)
		if !ok {
			continue
		}
		info := manifestInfo{
			Path: avroGetString(m, "manifest_path"),
		}
		if c, ok := m["content"]; ok {
			info.Content = avroToInt(c)
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func readManifest(data []byte) ([]manifestEntry, error) {
	reader, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	var entries []manifestEntry
	for reader.Scan() {
		record, err := reader.Read()
		if err != nil {
			return nil, err
		}
		m, ok := record.(map[string]any)
		if !ok {
			continue
		}

		e := manifestEntry{
			Status: avroToInt(m["status"]),
		}

		if df, ok := m["data_file"].(map[string]any); ok {
			e.Content = avroToInt(df["content"])
			e.FilePath = avroGetString(df, "file_path")
		}

		entries = append(entries, e)
	}
	return entries, nil
}

func avroGetString(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func avroToInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int32:
		return int(x)
	case int64:
		return int(x)
	default:
		return 0
	}
}

// --- Parquet reading ---

func readParquetRows(data []byte) ([]map[string]any, error) {
	reader := pq.NewReader(bytes.NewReader(data))

	colNames := make([]string, len(reader.Schema().Fields()))
	for i, f := range reader.Schema().Fields() {
		colNames[i] = f.Name()
	}

	var rows []map[string]any
	rowBuf := make([]pq.Row, 1)
	for {
		n, _ := reader.ReadRows(rowBuf)
		if n == 0 {
			break
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

func parquetValueToGo(v pq.Value) any {
	switch v.Kind() {
	case pq.Boolean:
		return v.Boolean()
	case pq.Int32:
		return int64(v.Int32())
	case pq.Int64:
		return v.Int64()
	case pq.Float:
		return float64(v.Float())
	case pq.Double:
		return v.Double()
	case pq.ByteArray, pq.FixedLenByteArray:
		return string(v.ByteArray())
	default:
		return v.String()
	}
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		var n int64
		fmt.Sscanf(x, "%d", &n)
		return n
	default:
		return 0
	}
}

// Keep the compiler happy — we reference time in logging.
var _ = time.Now
