package message

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"spitha/datagen/datagen/logger"
	"spitha/datagen/datagen/message/quickstart"
	"spitha/datagen/datagen/value"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
)

/**********************************************************************
**                                                                   **
**                          Jitter setting                           **
**                                                                   **
***********************************************************************/
func MakeRatePerSecondJitter(produceMode string, defaultValue int, jitterRate float64) int {
	if jitterRate == 0 {
		return defaultValue
	}
	switch produceMode {
	case value.PRODUCE_MODE_INTERVAL:
		min := defaultValue - int((jitterRate)*float64(defaultValue))
		max := defaultValue + int((jitterRate)*float64(defaultValue))
		diff := max - min
		randNum, _ := rand.Int(rand.Reader, big.NewInt(int64(diff+1)))
		result := min + int(randNum.Int64())
		if result == 0 {
			return 1
		}
		return result
	case value.PRODUCE_MODE_RATE_PER_SEC:
		min := defaultValue - int((jitterRate)*float64(defaultValue))
		max := defaultValue + int((jitterRate)*float64(defaultValue))
		diff := max - min
		randNum, _ := rand.Int(rand.Reader, big.NewInt(int64(diff+1)))
		result := min + int(randNum.Int64())
		if result == 0 {
			return 1
		}
		return result
	case value.PRODUCE_MODE_DATA_RATE_LIMIT_BPS:
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
**                            Make Message                           **
**                                                                   **
***********************************************************************/
func MakeMessage(serde *sr.Serde, messageMode string, quickstartType string, messageBytes int, schemaRegistryMessageType string) *kgo.Record {
	switch messageMode {
	case value.MESSAGE_MODE_QUICKSTART:
		return makeQuickstartMessage(serde, quickstartType, schemaRegistryMessageType)
	case value.MESSAGE_MODE_MESSAGE_BYTES:
		return makeMessageBytes(messageBytes)
	}
	return &kgo.Record{}
}

/**********************************************************************
**                                                                   **
**                       Make Quickstart Message                     **
**                                                                   **
***********************************************************************/
func makeQuickstartMessage(serde *sr.Serde, quickstartType string, schemaRegistryMessageType string) *kgo.Record {
	var msgValue interface{}
	var msgKey interface{}
	switch quickstartType {
	case value.QUICKSTART_USER:
		switch schemaRegistryMessageType {
		case value.SCHEMA_REGISTRY_MEESAGE_TYPE_PROTOUBUF:
			randomData := quickstart.MakeRandomPersonForProtoBuf()
			msgValue = randomData
			msgKey = randomData.GetFirstName()
		default:
			randomData := quickstart.MakeRandomPerson()
			msgValue = randomData
			msgKey = randomData.FirstName
		}
	case value.QUICKSTART_BOOK:
		switch schemaRegistryMessageType {
		case value.SCHEMA_REGISTRY_MEESAGE_TYPE_PROTOUBUF:
			randomData := quickstart.MakeRandomBookForProtoBuf()
			msgValue = randomData
			msgKey = randomData.GetGenre()
		default:
			randomData := quickstart.MakeRandomBook()
			msgValue = randomData
			msgKey = randomData.Genre
		}
	case value.QUICKSTART_CAR:
		switch schemaRegistryMessageType {
		case value.SCHEMA_REGISTRY_MEESAGE_TYPE_PROTOUBUF:
			randomData := quickstart.MakeRandomCarForProtoBuf()
			msgValue = randomData
			msgKey = randomData.GetBrand()
		default:
			randomData := quickstart.MakeRandomCar()
			msgValue = randomData
			msgKey = randomData.Brand
		}
	case value.QUICKSTART_ADDRESS:
		switch schemaRegistryMessageType {
		case value.SCHEMA_REGISTRY_MEESAGE_TYPE_PROTOUBUF:
			randomData := quickstart.MakeRandomAddressForProtoBuf()
			msgValue = randomData
			msgKey = randomData.GetCountry()
		default:
			randomData := quickstart.MakeRandomAddress()
			msgValue = randomData
			msgKey = randomData.Country
		}
	case value.QUICKSTART_CONTACT:
		switch schemaRegistryMessageType {
		case value.SCHEMA_REGISTRY_MEESAGE_TYPE_PROTOUBUF:
			randomData := quickstart.MakeRandomContactForProtoBuf()
			msgValue = randomData
			msgKey = randomData.GetEmail()
		default:
			randomData := quickstart.MakeRandomContact()
			msgValue = randomData
			msgKey = randomData.Email
		}
	case value.QUICKSTART_MOVIE:
		switch schemaRegistryMessageType {
		case value.SCHEMA_REGISTRY_MEESAGE_TYPE_PROTOUBUF:
			randomData := quickstart.MakeRandomMovieForProtoBuf()
			msgValue = randomData
			msgKey = randomData.GetGenre()
		default:
			randomData := quickstart.MakeRandomMovie()
			msgValue = randomData
			msgKey = randomData.Genre
		}
	case value.QUICKSTART_JOB:
		switch schemaRegistryMessageType {
		case value.SCHEMA_REGISTRY_MEESAGE_TYPE_PROTOUBUF:
			randomData := quickstart.MakeRandomJobForProtoBuf()
			msgValue = randomData
			msgKey = randomData.GetTitle()
		default:
			randomData := quickstart.MakeRandomJob()
			msgValue = randomData
			msgKey = randomData.Title
		}
	}
	msgByteValue, err := json.Marshal(msgValue)
	if err != nil {
		logger.Log.Error(fmt.Sprintln(err))
		return &kgo.Record{}
	}
	msgByteKey, err := json.Marshal(msgKey)
	if err != nil {
		logger.Log.Error(fmt.Sprintln(err))
		return &kgo.Record{}
	}
	// schema registry
	if schemaRegistryMessageType != "" {
		if serde == nil {
			panic("serde is nil")
		}
		return &kgo.Record{
			Key:       msgByteKey,
			Value:     serde.MustEncode(msgValue),
			Timestamp: time.Now(),
		}
	} else {
		return &kgo.Record{
			Key:       msgByteKey,
			Value:     msgByteValue,
			Timestamp: time.Now(),
		}
	}
}

/**********************************************************************
**                                                                   **
**                          Make Message Bytes                       **
**                                                                   **
***********************************************************************/
func makeMessageBytes(messageMaxBytes int) *kgo.Record {
	return kgo.SliceRecord([]byte(strings.Repeat("A", messageMaxBytes)))
}
