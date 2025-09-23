package protobuf

import (
	"context"
	"embed"
	"fmt"
	"spitha/datagen/datagen/value"

	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
)

//go:embed *.proto
var protoFS embed.FS

func HelperProtobuf(quickstartType string, srClient *sr.Client, serde *sr.Serde, subjectName string) {
	type ent struct {
		file   string
		sample proto.Message
		index  int
	}
	m := map[string]ent{
		value.QUICKSTART_USER:    {"person.proto", &PersonInfo{}, 0},
		value.QUICKSTART_BOOK:    {"book.proto", &BookInfo{}, 0},
		value.QUICKSTART_CAR:     {"car.proto", &CarInfo{}, 0},
		value.QUICKSTART_ADDRESS: {"address.proto", &AddressInfo{}, 0},
		value.QUICKSTART_CONTACT: {"contact.proto", &ContactInfo{}, 0},
		value.QUICKSTART_MOVIE:   {"movie.proto", &MovieInfo{}, 0},
		value.QUICKSTART_JOB:     {"job.proto", &JobInfo{}, 0},
	}

	e, ok := m[quickstartType]
	if !ok {
		panic(fmt.Errorf("unknown quickstart type: %s", quickstartType))
	}

	protoBytes, err := protoFS.ReadFile(e.file)
	if err != nil {
		panic(fmt.Errorf("read proto file %q: %w", e.file, err))
	}
	protoText := string(protoBytes)

	ss, err := srClient.CreateSchema(context.Background(), subjectName, sr.Schema{
		Schema: protoText,
		Type:   sr.TypeProtobuf,
	})
	if err != nil {
		panic(err)
	}

	serde.Register(
		ss.ID,
		e.sample,
		sr.EncodeFn(func(v any) ([]byte, error) {
			pm, ok := v.(proto.Message)
			if !ok {
				return nil, fmt.Errorf("expected proto.Message, got %T", v)
			}
			return proto.Marshal(pm)
		}),
		sr.DecodeFn(func(b []byte, v any) error {
			pm, ok := v.(proto.Message)
			if !ok {
				return fmt.Errorf("expected proto.Message target, got %T", v)
			}
			return proto.Unmarshal(b, pm)
		}),
		sr.Index(e.index),
	)
}
