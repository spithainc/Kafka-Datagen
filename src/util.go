package src

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
)

var (
	TotalTime     time.Duration = 0
	MinLatency    time.Duration = time.Duration(int64(1<<63 - 1))
	MaxLatency    time.Duration = 0
	AvgLatency    time.Duration = 0
	MessageNumber uint64        = 0
)

/**********************************************************************
**                                                                   **
**                            Check Topic                            **
**                                                                   **
***********************************************************************/
func checkTopic(ctx context.Context, opts []kgo.Opt) error {
	var adminClient *kadm.Client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer client.Close()
	adminClient = kadm.NewClient(client)
	topicList, err := adminClient.ListTopics(ctx, Module.Topic.Name)
	if err != nil {
		if err != nil {
			return err
		}
	}

	// no topic
	if !topicList.Has(Module.Topic.Name) {
		partition := int32(3)     // default partition
		replicafactor := int16(1) // default replicafactor
		if Module.Topic.Partition != "" {
			partition = int32(stringToInt(Module.Topic.Partition))
		}
		if Module.Topic.Replicafactor != "" {
			replicafactor = int16(stringToInt(Module.Topic.Replicafactor))
		}
		_, err := adminClient.CreateTopic(ctx, partition, replicafactor, nil, Module.Topic.Name)
		if err != nil {
			return err
		}
		time.Sleep(time.Second)
	}
	client.Close()

	defer client.Close()
	return nil
}

/**********************************************************************
**                                                                   **
**                            Metric print                           **
**                                                                   **
***********************************************************************/
func metricTicker(ticker *time.Ticker) {
	for range ticker.C {
		if MessageNumber != 0 {
			Log.Info(fmt.Sprintln("min latency : ", MinLatency))
			Log.Info(fmt.Sprintln("max latency : ", MaxLatency))
			Log.Info(fmt.Sprintln("avg latency : ", TotalTime/time.Duration(MessageNumber)))
			Log.Info(fmt.Sprintln("number messages : ", MessageNumber))
			fmt.Println()

			// init values
			MinLatency = time.Duration(int64(1<<63 - 1))
			MaxLatency = 0
			AvgLatency = 0
			MessageNumber = 0
			TotalTime = 0
		} else {
			Log.Info(fmt.Sprintln("number messages : ", 0))
		}
	}
}

/**********************************************************************
**                                                                   **
**                           Check latency                           **
**                                                                   **
***********************************************************************/
func checkElapsedLatency(elapsed time.Duration) {
	// min time elapsed
	if elapsed < MinLatency {
		MinLatency = elapsed
	}

	// max time elapsed
	if elapsed > MaxLatency {
		MaxLatency = elapsed
	}

	// total time for avg latency
	TotalTime += elapsed

	// message number
	MessageNumber++
}

/**********************************************************************
**                                                                   **
**                          Jitter setting                           **
**                                                                   **
***********************************************************************/
func makeRatePerSecondJitter(producerType int, defaultValue int, jitterRate float64) int {
	if jitterRate == 0 {
		return defaultValue
	}
	switch producerType {
	case PRODUCE_TYPE_INTERVAL:
		min := defaultValue - int((jitterRate)*float64(defaultValue))
		max := defaultValue + int((jitterRate)*float64(defaultValue))
		diff := max - min
		randNum, _ := rand.Int(rand.Reader, big.NewInt(int64(diff+1)))
		result := min + int(randNum.Int64())
		if result == 0 {
			return 1
		}
		return result
	case PRODUCE_TYPE_RATE_PER_SEC:
		min := defaultValue - int((jitterRate)*float64(defaultValue))
		max := defaultValue + int((jitterRate)*float64(defaultValue))
		diff := max - min
		randNum, _ := rand.Int(rand.Reader, big.NewInt(int64(diff+1)))
		result := min + int(randNum.Int64())
		if result == 0 {
			return 1
		}
		return result
	case PRODUCE_TYPE_LIMIT_DATA_AMOUNT_PER_SEC:
		min := defaultValue - int((jitterRate)*float64(defaultValue))
		max := defaultValue + int((jitterRate)*float64(defaultValue))
		diff := max - min
		randNum, _ := rand.Int(rand.Reader, big.NewInt(int64(diff+1)))
		result := min + int(randNum.Int64())
		if result == 0 {
			return 1
		}
		return result
	}
	return defaultValue
}

/**********************************************************************
**                                                                   **
**                            Gen Message                            **
**                                                                   **
***********************************************************************/
func makeMessage(produceSettings *ProduceSettings, serde *sr.Serde) *kgo.Record {
	var msgValue interface{}
	var msgKey interface{}

	if produceSettings.MessageType == MESSAGE_TYPE_QUICKSTART {
		switch produceSettings.MessageSettings.Quickstart {
		case "user":
			randomData := makeRandomPerson()
			msgValue = randomData
			msgKey = randomData.FirstName
		case "book":
			randomData := makeRandomBook()
			msgValue = randomData
			msgKey = randomData.Genre
		case "car":
			randomData := makeRandomCar()
			msgValue = randomData
			msgKey = randomData.Brand
		case "address":
			randomData := makeRandomAddress()
			msgValue = randomData
			msgKey = randomData.Country
		case "contact":
			randomData := makeRandomContact()
			msgValue = randomData
			msgKey = randomData.Email
		case "movie":
			randomData := makeRandomMovie()
			msgValue = randomData
			msgKey = randomData.Genre
		case "job":
			randomData := makeRandomJob()
			msgValue = randomData
			msgKey = randomData.Title
		}
		msgByteValue, err := json.Marshal(msgValue)
		if err != nil {
			Log.Error(fmt.Sprintln(err))
			return &kgo.Record{}
		}
		msgByteKey, err := json.Marshal(msgKey)
		if err != nil {
			Log.Error(fmt.Sprintln(err))
			return &kgo.Record{}
		}

		// schema registry
		if Module.Producer.SchemaRegistry.Server.Urls != "" {
			return &kgo.Record{
				Key:       msgByteKey,
				Value:     serde.MustEncode(msgValue),
				Timestamp: time.Now(),
			}
		}

		return &kgo.Record{
			Key:       msgByteKey,
			Value:     msgByteValue,
			Timestamp: time.Now(),
		}
	} else if produceSettings.MessageType == MESSAGE_TYPE_MESSAGE_BYTES {
		return kgo.SliceRecord(make([]byte, produceSettings.MessageSettings.MessageBytes)) // use message byte
	}

	return &kgo.Record{}

}

/**********************************************************************
**                                                                   **
**                            Type utils                             **
**                                                                   **
***********************************************************************/
func stringToInt(value string) int {
	i, err := strconv.Atoi(value)
	if err != nil {
		Log.Error(fmt.Sprintln(err))
		return 0
	}
	return i
}

func stringToFloat64(value string) float64 {

	i, err := strconv.ParseFloat(value, 64)
	if err != nil {
		Log.Error(fmt.Sprintln(err))
		return 0
	}
	return i
}

/**********************************************************************
**                                                                   **
**                      Schema Registry utils                        **
**                                                                   **
***********************************************************************/
// avroTypeMap maps Go types to AVRO types.
var avroTypeMap = map[reflect.Kind]string{
	reflect.String:  "string",
	reflect.Int:     "int",
	reflect.Int32:   "int",
	reflect.Int64:   "long",
	reflect.Float32: "float",
	reflect.Float64: "double",
	reflect.Bool:    "boolean",
}

// generate avro schema for schema registry temlpate
func generateAvroSchema(schemaTemplate string, src interface{}) (string, error) {
	schema, err := generateFieldSchema(reflect.TypeOf(src))
	if err != nil {
		return "", err
	}

	// schema template json parsing
	var template map[string]interface{}
	if err := json.Unmarshal([]byte(schemaTemplate), &template); err != nil {
		return "", err
	}

	// add fileds
	template["fields"] = schema

	// schema to json
	schemaBytes, err := json.Marshal(template)
	if err != nil {
		return "", err
	}

	return string(schemaBytes), nil
}

func generateFieldSchema(t reflect.Type) ([]map[string]interface{}, error) {
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("source is not a struct")
	}

	var fields []map[string]interface{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldType := field.Type

		var avroType interface{}
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}

		if fieldType.Kind() == reflect.Struct {
			nestedFields, err := generateFieldSchema(fieldType)
			if err != nil {
				return nil, err
			}
			avroType = map[string]interface{}{
				"type":   "record",
				"name":   fieldType.Name(),
				"fields": nestedFields,
			}
		} else {
			var exists bool
			avroType, exists = avroTypeMap[fieldType.Kind()]
			if !exists {
				return nil, fmt.Errorf("unsupported field type: %s", fieldType.Name())
			}
		}
		fieldName := field.Tag.Get("json")
		if fieldName == "" {
			fieldName = field.Name
		}
		fields = append(fields, map[string]interface{}{"name": fieldName, "type": avroType})
	}

	return fields, nil
}

/**********************************************************************
**                                                                   **
**                            Check Params                           **
**                                                                   **
***********************************************************************/
func checkParams(p ...interface{}) bool {
	count := 0
	for _, param := range p {
		switch v := param.(type) {
		case int, float64:
			if v != 0 {
				count++
				if count > 1 {
					return false
				}
			}
		case string:
			if v != "" {
				count++
				if count > 1 {
					return false
				}
			}
		}
	}
	return count == 1
}
