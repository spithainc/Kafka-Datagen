package src

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/twmb/franz-go/pkg/kgo"
)

func makeRatePerSecondJitter(ratePerSecond int, jitterRate float64) int {
	if jitterRate == 0 {
		return ratePerSecond
	}
	min := ratePerSecond - int((jitterRate)*float64(ratePerSecond))
	max := ratePerSecond + int((jitterRate)*float64(ratePerSecond))
	diff := max - min
	randNum, _ := rand.Int(rand.Reader, big.NewInt(int64(diff+1)))
	result := min + int(randNum.Int64())
	if result == 0 {
		return 1
	}
	return result
}

func makeMessage(quickStart string, messageByte int) kgo.Record {
	var valueData interface{}
	switch quickStart {
	case "user":
		valueData = gofakeit.Person()
	case "book":
		valueData = gofakeit.Book()
	case "car":
		valueData = gofakeit.Car()
	case "address":
		valueData = gofakeit.Address()
	case "contact":
		valueData = gofakeit.Contact()
	case "movie":
		valueData = gofakeit.Movie()
	case "job":
		valueData = gofakeit.Job()
	default:
		b := make([]byte, messageByte)
		_, err := rand.Read(b)
		if err != nil {
			Log.Error(fmt.Sprintln(err))
		}
		valueData = b
	}
	byteData, err := json.Marshal(valueData)
	if err != nil {
		Log.Error(fmt.Sprintln(err))
		return kgo.Record{}
	}
	return kgo.Record{
		Value:     byteData,
		Timestamp: time.Now(),
	}
}

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

func findMinDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	min := durations[0]
	for _, duration := range durations {
		if duration < min {
			min = duration
		}
	}
	return min
}

func findMaxDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	max := durations[0]
	for _, duration := range durations {
		if duration > max {
			max = duration
		}
	}
	return max
}

func findAverageDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	var total time.Duration
	for _, duration := range durations {
		total += duration
	}
	return total / time.Duration(len(durations))
}
