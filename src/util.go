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

var (
	TotalTime     time.Duration = 0
	MinLatency    time.Duration = time.Duration(int64(1<<63 - 1))
	MaxLatency    time.Duration = 0
	AvgLatency    time.Duration = 0
	MessageNumber uint64        = 0
)

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

func makeRatePerSecondJitter(producerType int, defaultValue int, jitterRate float64) int {
	if jitterRate == 0 {
		return defaultValue
	}
	switch producerType {
	case PRODUCE_INTERVAL:
		min := defaultValue - int((jitterRate)*float64(defaultValue))
		max := defaultValue + int((jitterRate)*float64(defaultValue))
		diff := max - min
		randNum, _ := rand.Int(rand.Reader, big.NewInt(int64(diff+1)))
		result := min + int(randNum.Int64())
		if result == 0 {
			return 1
		}
		return result
	case PRODUCE_RATE_PER_SEC:
		min := defaultValue - int((jitterRate)*float64(defaultValue))
		max := defaultValue + int((jitterRate)*float64(defaultValue))
		diff := max - min
		randNum, _ := rand.Int(rand.Reader, big.NewInt(int64(diff+1)))
		result := min + int(randNum.Int64())
		if result == 0 {
			return 1
		}
		return result
	case PRODUCE_LIMIT_DATA_AMOUNT_PER_SEC:
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

func makeMessage(quickStart string, messageByte int) *kgo.Record {
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
		return kgo.SliceRecord(make([]byte, messageByte))
	}
	byteData, err := json.Marshal(valueData)
	if err != nil {
		Log.Error(fmt.Sprintln(err))
		return &kgo.Record{}
	}
	return &kgo.Record{
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
