package sink

import (
	"fmt"
	"strings"
	"time"

	"github.com/pg2iceberg/pg2iceberg/schema"
)

// PartitionField describes a single partition field in an Iceberg partition spec.
type PartitionField struct {
	SourceID  int    // Iceberg field ID of the source column
	FieldID   int    // Partition field ID (1000+)
	Name      string // Partition field name (e.g. "created_at_day")
	Transform string // identity, year, month, day, hour
}

// PartitionSpec describes the partition spec for an Iceberg table.
type PartitionSpec struct {
	Fields []PartitionField
}

// parsePartitionExpr parses "day(created_at)" into ("day", "created_at"),
// or "region" into ("identity", "region").
func parsePartitionExpr(expr string) (transform, column string, err error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", "", fmt.Errorf("empty partition expression")
	}

	if idx := strings.Index(expr, "("); idx > 0 {
		if !strings.HasSuffix(expr, ")") {
			return "", "", fmt.Errorf("invalid partition expression %q: missing closing parenthesis", expr)
		}
		transform = strings.ToLower(expr[:idx])
		column = strings.TrimSpace(expr[idx+1 : len(expr)-1])
		if column == "" {
			return "", "", fmt.Errorf("invalid partition expression %q: empty column name", expr)
		}
		return transform, column, nil
	}

	return "identity", expr, nil
}

// BuildPartitionSpec builds a PartitionSpec from config expressions and table schema.
// Each expression is either "transform(column)" (e.g. "day(created_at)") or just
// "column" for identity transform (e.g. "region").
func BuildPartitionSpec(exprs []string, ts *schema.TableSchema) (*PartitionSpec, error) {
	if len(exprs) == 0 {
		return &PartitionSpec{}, nil
	}

	spec := &PartitionSpec{}
	for i, expr := range exprs {
		transform, colName, err := parsePartitionExpr(expr)
		if err != nil {
			return nil, err
		}

		col := findColumn(ts, colName)
		if col == nil {
			return nil, fmt.Errorf("partition column %q not found in table %s", colName, ts.Table)
		}

		switch transform {
		case "identity", "year", "month", "day", "hour":
			// valid
		default:
			return nil, fmt.Errorf("unsupported partition transform %q in %q", transform, expr)
		}

		// Validate that time transforms are used on date/timestamp columns.
		if transform != "identity" {
			if !isTemporalType(col.PGType) {
				return nil, fmt.Errorf("partition transform %q requires a date/timestamp column, got %q (%s)", transform, colName, col.PGType)
			}
		}

		name := colName
		if transform != "identity" {
			name = colName + "_" + transform
		}

		spec.Fields = append(spec.Fields, PartitionField{
			SourceID:  col.FieldID,
			FieldID:   1000 + i,
			Name:      name,
			Transform: transform,
		})
	}
	return spec, nil
}

// IsUnpartitioned returns true if the spec has no partition fields.
func (ps *PartitionSpec) IsUnpartitioned() bool {
	return len(ps.Fields) == 0
}

// PartitionKey computes the partition key for a row.
// Returns a string key suitable for use as a map key, and the individual partition values.
func (ps *PartitionSpec) PartitionKey(row map[string]any, ts *schema.TableSchema) (string, map[string]any) {
	if ps.IsUnpartitioned() {
		return "", nil
	}

	values := make(map[string]any, len(ps.Fields))
	parts := make([]string, len(ps.Fields))

	for i, field := range ps.Fields {
		col := findColumnByID(ts, field.SourceID)
		if col == nil {
			parts[i] = "__null__"
			values[field.Name] = nil
			continue
		}

		raw := row[col.Name]
		if raw == nil {
			parts[i] = "__null__"
			values[field.Name] = nil
			continue
		}

		val := applyTransform(field.Transform, raw, col.PGType)
		values[field.Name] = val
		parts[i] = fmt.Sprintf("%v", val)
	}

	return strings.Join(parts, "/"), values
}

// PartitionPath returns the S3 directory path component for a partition.
func (ps *PartitionSpec) PartitionPath(partValues map[string]any) string {
	if ps.IsUnpartitioned() || len(partValues) == 0 {
		return ""
	}

	parts := make([]string, 0, len(ps.Fields))
	for _, field := range ps.Fields {
		val := partValues[field.Name]
		if val == nil {
			parts = append(parts, fmt.Sprintf("%s=__null__", field.Name))
		} else {
			parts = append(parts, fmt.Sprintf("%s=%v", field.Name, val))
		}
	}
	return strings.Join(parts, "/")
}

// PartitionSpecJSON returns the partition spec fields as a JSON array string for manifest metadata.
func (ps *PartitionSpec) PartitionSpecJSON() string {
	if ps.IsUnpartitioned() {
		return "[]"
	}

	fields := make([]string, len(ps.Fields))
	for i, f := range ps.Fields {
		fields[i] = fmt.Sprintf(`{"source-id": %d, "field-id": %d, "name": "%s", "transform": "%s"}`,
			f.SourceID, f.FieldID, f.Name, f.Transform)
	}
	return "[" + strings.Join(fields, ", ") + "]"
}

// CatalogPartitionSpec returns the partition spec as a map for the Iceberg REST catalog API.
func (ps *PartitionSpec) CatalogPartitionSpec() map[string]any {
	fields := make([]any, len(ps.Fields))
	for i, f := range ps.Fields {
		fields[i] = map[string]any{
			"field-id":  f.FieldID,
			"source-id": f.SourceID,
			"transform": f.Transform,
			"name":      f.Name,
		}
	}
	return map[string]any{
		"spec-id": 0,
		"fields":  fields,
	}
}

// PartitionRecordSchemaAvro returns the Avro schema for the partition tuple in manifest entries.
func (ps *PartitionSpec) PartitionRecordSchemaAvro(ts *schema.TableSchema) string {
	if ps.IsUnpartitioned() {
		return `{"type": "record", "name": "r102", "fields": []}`
	}

	fields := make([]string, len(ps.Fields))
	for i, f := range ps.Fields {
		avroType := partitionFieldAvroType(f.Transform, ts, f.SourceID)
		fields[i] = fmt.Sprintf(`{"name": "%s", "type": ["null", "%s"], "default": null, "field-id": %d}`,
			f.Name, avroType, f.FieldID)
	}

	return fmt.Sprintf(`{"type": "record", "name": "r102", "fields": [%s]}`, strings.Join(fields, ", "))
}

// PartitionAvroValue returns the partition values formatted for Avro encoding in manifest entries.
func (ps *PartitionSpec) PartitionAvroValue(partValues map[string]any, ts *schema.TableSchema) map[string]any {
	if ps.IsUnpartitioned() || len(partValues) == 0 {
		return map[string]any{}
	}

	result := make(map[string]any, len(ps.Fields))
	for _, f := range ps.Fields {
		val := partValues[f.Name]
		if val == nil {
			result[f.Name] = nil
			continue
		}
		avroType := partitionFieldAvroType(f.Transform, ts, f.SourceID)
		result[f.Name] = goavroUnion(avroType, val)
	}
	return result
}

// --- helpers ---

func findColumn(ts *schema.TableSchema, name string) *schema.Column {
	for i := range ts.Columns {
		if ts.Columns[i].Name == name {
			return &ts.Columns[i]
		}
	}
	return nil
}

func findColumnByID(ts *schema.TableSchema, fieldID int) *schema.Column {
	for i := range ts.Columns {
		if ts.Columns[i].FieldID == fieldID {
			return &ts.Columns[i]
		}
	}
	return nil
}

func isTemporalType(pgType string) bool {
	switch strings.ToLower(pgType) {
	case "date", "timestamp", "timestamp without time zone",
		"timestamptz", "timestamp with time zone":
		return true
	}
	return false
}

// applyTransform applies a partition transform to a value.
func applyTransform(transform string, value any, pgType string) any {
	switch transform {
	case "identity":
		return value
	case "year":
		t := toTime(value, pgType)
		if t.IsZero() {
			return nil
		}
		return int32(t.Year() - 1970)
	case "month":
		t := toTime(value, pgType)
		if t.IsZero() {
			return nil
		}
		return int32((t.Year()-1970)*12 + int(t.Month()) - 1)
	case "day":
		t := toTime(value, pgType)
		if t.IsZero() {
			return nil
		}
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		return int32(t.Sub(epoch).Hours() / 24)
	case "hour":
		t := toTime(value, pgType)
		if t.IsZero() {
			return nil
		}
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		return int32(t.Sub(epoch).Hours())
	default:
		return value
	}
}

// toTime converts a value to time.Time for partition transforms.
func toTime(v any, pgType string) time.Time {
	switch x := v.(type) {
	case time.Time:
		return x
	case string:
		// Try date first.
		if strings.ToLower(pgType) == "date" {
			if t, err := time.Parse("2006-01-02", x); err == nil {
				return t
			}
		}
		// Try timestamp formats.
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999-07:00",
			"2006-01-02 15:04:05.999999-07",
			"2006-01-02 15:04:05.999999+00",
			"2006-01-02 15:04:05-07:00",
			"2006-01-02 15:04:05-07",
			"2006-01-02 15:04:05+00",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, x); err == nil {
				return t
			}
		}
	}
	return time.Time{}
}

// partitionFieldAvroType returns the Avro type for a partition field's transformed value.
func partitionFieldAvroType(transform string, ts *schema.TableSchema, sourceID int) string {
	switch transform {
	case "year", "month", "day", "hour":
		return "int"
	case "identity":
		col := findColumnByID(ts, sourceID)
		if col == nil {
			return "string"
		}
		return icebergToAvroType(schema.IcebergType(col.PGType))
	default:
		return "string"
	}
}

// icebergToAvroType maps Iceberg primitive types to Avro types.
func icebergToAvroType(icebergType string) string {
	switch icebergType {
	case "int":
		return "int"
	case "long":
		return "long"
	case "float":
		return "float"
	case "double":
		return "double"
	case "boolean":
		return "boolean"
	case "date":
		return "int"
	case "timestamp", "timestamptz":
		return "long"
	default:
		return "string"
	}
}

// goavroUnion wraps a Go value in a goavro union map.
func goavroUnion(avroType string, val any) map[string]any {
	// Ensure the Go type matches what goavro expects.
	switch avroType {
	case "int":
		return map[string]any{"int": toAvroInt32Value(val)}
	case "long":
		return map[string]any{"long": toAvroInt64Value(val)}
	case "float":
		return map[string]any{"float": toAvroFloat32Value(val)}
	case "double":
		return map[string]any{"double": toAvroFloat64Value(val)}
	case "boolean":
		return map[string]any{"boolean": toBool(val)}
	default:
		return map[string]any{"string": toString(val)}
	}
}

func toAvroInt32Value(v any) int32 {
	switch x := v.(type) {
	case int32:
		return x
	case int:
		return int32(x)
	case int64:
		return int32(x)
	case float64:
		return int32(x)
	default:
		return 0
	}
}

func toAvroInt64Value(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}

func toAvroFloat32Value(v any) float32 {
	switch x := v.(type) {
	case float32:
		return x
	case float64:
		return float32(x)
	default:
		return 0
	}
}

func toAvroFloat64Value(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	default:
		return 0
	}
}
