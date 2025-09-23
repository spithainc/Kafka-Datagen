package avro

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"spitha/datagen/datagen/logger"
	"spitha/datagen/datagen/message/quickstart"
	"spitha/datagen/datagen/value"
	"strings"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/sr"
)

func HelperAvro(quickstartType string, srClient *sr.Client, serde *sr.Serde, subjectName string) {
	// avro
	// schema template
	var schemaTemplate = `{
					"type": "record",
					"name": "datagen_spitha",
					"namespace": "datagen.spitha.io",
					"fields" : []
				}`
	var schemaSrc interface{}
	switch quickstartType {
	case value.QUICKSTART_USER:
		schemaSrc = quickstart.PersonInfo{}
	case value.QUICKSTART_BOOK:
		schemaSrc = quickstart.BookInfo{}
	case value.QUICKSTART_CAR:
		schemaSrc = quickstart.CarInfo{}
	case value.QUICKSTART_ADDRESS:
		schemaSrc = quickstart.AddressInfo{}
	case value.QUICKSTART_CONTACT:
		schemaSrc = quickstart.ContactInfo{}
	case value.QUICKSTART_MOVIE:
		schemaSrc = quickstart.MovieInfo{}
	case value.QUICKSTART_JOB:
		schemaSrc = quickstart.JobInfo{}
	}

	// add quickstart field
	schema, err := generateAvroSchema(schemaTemplate, schemaSrc)
	if err != nil {
		logger.Log.Error(fmt.Sprintln("Error generating schema:", err))
		panic(err)
	}
	logger.Log.Debug(schema)

	// find schema in schema registry
	schemaRegistrySchema, err := srClient.CreateSchema(context.Background(), subjectName, sr.Schema{
		Schema: schema,
		Type:   sr.TypeAvro,
	})
	if err != nil {
		panic(err)
	}
	logger.Log.Info(fmt.Sprintf("created or reusing schema subject %q version %d id %d\n", schemaRegistrySchema.Subject, schemaRegistrySchema.Version, schemaRegistrySchema.ID))

	// avro parse
	avroSchema, err := avro.Parse(schema)
	if err != nil {
		panic(err)
	}

	// serde register
	serde.Register(
		schemaRegistrySchema.ID,
		schemaSrc,
		sr.EncodeFn(func(v any) ([]byte, error) {
			return avro.Marshal(avroSchema, v)
		}),
		sr.DecodeFn(func(b []byte, v any) error {
			return avro.Unmarshal(avroSchema, b, v)
		}),
	)
}

// generate avro schema for schema registry template
func generateAvroSchema(schemaTemplate string, src interface{}) (string, error) {

	t := reflect.TypeOf(src)
	if t == nil {
		return "", fmt.Errorf("src is nil")
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return "", fmt.Errorf("source must be a struct, got %s", t.Kind())
	}

	fields, err := generateFieldSchema(t)
	if err != nil {
		return "", err
	}

	var template map[string]interface{}
	if err := json.Unmarshal([]byte(schemaTemplate), &template); err != nil {
		return "", fmt.Errorf("schema template unmarshal: %w", err)
	}
	template["fields"] = fields

	schemaBytes, err := json.Marshal(template)
	if err != nil {
		return "", fmt.Errorf("marshal final schema: %w", err)
	}
	return string(schemaBytes), nil
}

// --- internals ---
func generateFieldSchema(t reflect.Type) ([]map[string]interface{}, error) {
	var out []map[string]interface{}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		// skip unexported fields
		if f.PkgPath != "" {
			continue
		}

		// parse json tag
		tag := f.Tag.Get("json")
		name, opts := parseJSONTagForAvro(tag)
		if name == "" {
			name = f.Name
		}
		if name == "-" {
			continue
		}

		baseType := f.Type
		nullable := false

		// pointer => nullable
		for baseType.Kind() == reflect.Ptr {
			baseType = baseType.Elem()
			nullable = true
		}
		// treat omitempty as nullable (optional; toggle if undesired)
		if opts["omitempty"] {
			nullable = true
		}

		avroType, err := avroTypeFor(baseType)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", name, err)
		}

		// wrap nullable union
		var fieldType interface{} = avroType
		fieldObj := map[string]interface{}{"name": name}

		if nullable {
			fieldType = []interface{}{"null", avroType}
			fieldObj["default"] = nil
		}
		fieldObj["type"] = fieldType

		out = append(out, fieldObj)
	}
	return out, nil
}

func avroTypeFor(t reflect.Type) (interface{}, error) {
	switch t.Kind() {
	case reflect.Bool:
		return "boolean", nil
	case reflect.String:
		return "string", nil
	case reflect.Int8, reflect.Int16, reflect.Int32:
		return "int", nil
	case reflect.Int, reflect.Int64:
		// safer choice for Go int (platform dependent)
		return "long", nil
	case reflect.Uint8:
		// Prefer bytes for []byte; single uint8 is unusual—map to int
		return "int", nil
	case reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
		// Avro has no unsigned; map to long
		return "long", nil
	case reflect.Float32:
		return "float", nil
	case reflect.Float64:
		return "double", nil

	case reflect.Slice:
		// []byte => bytes
		if t.Elem().Kind() == reflect.Uint8 {
			return "bytes", nil
		}
		items, err := avroTypeFor(deref(t.Elem()))
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{
			"type":  "array",
			"items": items,
		}, nil

	case reflect.Array:
		// Avro에 고정 길이 배열은 없고, logical fixed 또는 array 선택.
		// 고정 바이트 배열이면 fixed로 매핑 가능하지만 여기선 array로 단순화.
		items, err := avroTypeFor(deref(t.Elem()))
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{
			"type":  "array",
			"items": items,
		}, nil

	case reflect.Map:
		// Avro maps require string keys
		if t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("map key must be string for Avro map, got %s", t.Key())
		}
		val, err := avroTypeFor(deref(t.Elem()))
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{
			"type":   "map",
			"values": val,
		}, nil

	case reflect.Struct:
		// time.Time => logicalType timestamp-millis
		if t.PkgPath() == "time" && t.Name() == "Time" {
			return map[string]interface{}{
				"type":        "long",
				"logicalType": "timestamp-millis",
			}, nil
		}
		// nested record
		fields, err := generateFieldSchema(t)
		if err != nil {
			return nil, err
		}
		name := t.Name()
		if name == "" {
			// anonymous struct: synthesize a name (best-effort)
			name = "Inline_" + sanitizeTypeString(t.String())
		}
		record := map[string]interface{}{
			"type":   "record",
			"name":   name,
			"fields": fields,
		}
		// Optional: include namespace from package path
		if ns := pkgToNamespace(t.PkgPath()); ns != "" {
			record["namespace"] = ns
		}
		return record, nil
	}

	return nil, fmt.Errorf("unsupported type: %s", t.String())
}

// helpers

func deref(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func parseJSONTagForAvro(tag string) (name string, opts map[string]bool) {
	opts = map[string]bool{}
	if tag == "" {
		return "", opts
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	for _, p := range parts[1:] {
		opts[p] = true
	}
	return name, opts
}

func pkgToNamespace(pkg string) string {
	// e.g., "github.com/org/project/pkg" -> "github.com.org.project.pkg"
	if pkg == "" {
		return ""
	}
	return strings.ReplaceAll(pkg, "/", ".")
}

func sanitizeTypeString(s string) string {
	// make it a valid Avro name-ish
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, ".", "_")
	s = strings.ReplaceAll(s, "[", "A")
	s = strings.ReplaceAll(s, "]", "Z")
	s = strings.ReplaceAll(s, "*", "P")
	s = strings.ReplaceAll(s, "{", "_")
	s = strings.ReplaceAll(s, "}", "_")
	s = strings.ReplaceAll(s, "(", "_")
	s = strings.ReplaceAll(s, ")", "_")
	return s
}
