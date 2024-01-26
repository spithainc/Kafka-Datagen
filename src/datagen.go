package src

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzap"
)

var (
	arrLatency []time.Duration
)

const (
	PRODUCE_INTERVAL                  = 0
	PRODUCE_RATE_PER_SEC              = 1
	PRODUCE_LIMIT_DATA_AMOUNT_PER_SEC = 2
)

func Datagen() {

	var (
		DEFAULT_WORK_TRHEAD  = 1
		DEFAULT_MESSAGE_SIZE = 100
		DEFAULT_INTERVAL     = 0
		DEFAUTL_JITTER_RATE  = 0.0
	)

	ctx := context.Background()
	opts := []kgo.Opt{}

	// setting for bootstrap server
	bootstrapServer := Module.BootstrapServer
	if bootstrapServer == "" {
		panic("please input producer.bootstrap.servers settings")
	}
	opts = append(opts, kgo.SeedBrokers(strings.Split(bootstrapServer, ",")...))
	opts = append(opts, kgo.WithLogger(kzap.New(Log)))
	opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	opts = append(opts, kgo.DisableIdempotentWrite())

	// setting for kafka auth
	opts, err := auth(opts)
	if err != nil {
		Log.Error(fmt.Sprintln(err))
		panic(err)
	}

	// setting for producer
	if Module.Producer.MaxMessageBytes != "" {
		opts = append(opts, kgo.ProducerBatchMaxBytes(int32(stringToInt(Module.Producer.MaxMessageBytes))))
	}
	if Module.Producer.Lingers != "" {
		opts = append(opts, kgo.ProducerLinger(time.Duration(stringToInt(Module.Producer.Lingers))))
	}
	if Module.Producer.CompressionType != "" {
		switch Module.Producer.CompressionType {
		case "uncompressed":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
		case "zstd":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
		case "lz4":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
		case "gzip":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
		case "snappy":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
		}
	}

	// setting for topic
	topic := Module.Topic.Name
	if topic == "" {
		panic("please input topic settings")
	}
	opts = append(opts, kgo.DefaultProduceTopic(topic))

	// setting for datagen
	quickstart := Module.Datagen.QuickStart
	produceType := PRODUCE_INTERVAL

	var ratePerSecond int
	if Module.Datagen.RatePerSecond != "" {
		ratePerSecond = stringToInt(Module.Datagen.RatePerSecond)
		produceType = PRODUCE_RATE_PER_SEC
	}
	Log.Info(fmt.Sprintln("produce type : ", produceType))
	workThread := DEFAULT_WORK_TRHEAD
	if Module.Datagen.GoRoutine != "" {
		workThread = stringToInt(Module.Datagen.GoRoutine)
	}
	messageBytes := DEFAULT_MESSAGE_SIZE
	if Module.Datagen.MessageBytes != "" {
		messageBytes = stringToInt(Module.Datagen.MessageBytes)
	}
	interval := DEFAULT_INTERVAL
	if Module.Datagen.Interval != "" {
		interval = stringToInt(Module.Datagen.Interval)
	}
	var limitPerSecond int
	if Module.Datagen.LimitDataAmountPerSecond != "" {
		limitPerSecond = stringToInt(Module.Datagen.LimitDataAmountPerSecond)
		produceType = PRODUCE_LIMIT_DATA_AMOUNT_PER_SEC
		workThread = DEFAULT_WORK_TRHEAD
	}
	ratepersecondJitter := DEFAUTL_JITTER_RATE
	if Module.Datagen.RatePerSecondJitter != "" {
		ratepersecondJitter = stringToFloat64(Module.Datagen.RatePerSecondJitter)
	}

	// ticker
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			Log.Info(fmt.Sprintln("min latency : ", findMinDuration(arrLatency)))
			Log.Info(fmt.Sprintln("max latency : ", findMaxDuration(arrLatency)))
			Log.Info(fmt.Sprintln("avg latency : ", findAverageDuration(arrLatency)))
			Log.Info(fmt.Sprintln("number messages : ", len(arrLatency)))
			arrLatency = []time.Duration{}
		}
	}()

	// check topic
	var adminClient *kadm.Client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	adminClient = kadm.NewClient(client)
	topicList, err := adminClient.ListTopics(ctx, Module.Topic.Name)
	if err != nil {
		if err != nil {
			panic(err)
		}
	}
	if !topicList.Has(Module.Topic.Name) {
		partition := int32(3)
		replicafactor := int16(1)
		if Module.Topic.Partition != "" {
			partition = int32(stringToInt(Module.Topic.Partition))
		}
		if Module.Topic.Replicafactor != "" {
			replicafactor = int16(stringToInt(Module.Topic.Replicafactor))
		}
		_, err := adminClient.CreateTopic(ctx, partition, replicafactor, nil, Module.Topic.Name)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}
	client.Close()

	// producer go routine
	var wg sync.WaitGroup
	wg.Add(workThread)
	produceJobs := make(chan int, workThread)
	for i := 1; i <= workThread; i++ {
		go worker(produceJobs, &wg, opts, ctx, produceType, ratePerSecond, interval, quickstart, messageBytes, limitPerSecond, ratepersecondJitter)
	}

	go func() {
		for {
			produceJobs <- 1
		}
	}()

	wg.Wait()
}

// Producer thread
func worker(jobs <-chan int, wg *sync.WaitGroup, opts []kgo.Opt, ctx context.Context, produceType int, ratePerSecond int, interval int, quickstart string, messageBytes int, limitPerSecond int, ratepersecondJitter float64) {

	// kafka client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	for range jobs {
		// Produce Messages
		switch produceType {
		case PRODUCE_INTERVAL:
			produceInterval(client, ctx, interval, quickstart, messageBytes)
		case PRODUCE_RATE_PER_SEC:
			produceRatePerSecond(client, ctx, ratePerSecond, quickstart, messageBytes, ratepersecondJitter)
		case PRODUCE_LIMIT_DATA_AMOUNT_PER_SEC:
			produceLimitPerSecond(client, ctx, limitPerSecond)
		default:
			Log.Info(fmt.Sprintln("the value is missing or invalid in the produce type"))
		}
	}
	// Flush
	if err := client.Flush(ctx); err != nil {
		Log.Error(fmt.Sprintln(err))
		panic(err)
	}
	wg.Done()
}

// Interval Producer
func produceInterval(client *kgo.Client, ctx context.Context, interval int, quickStart string, messageBytes int) {
	latencyStart := time.Now()
	message := makeMessage(quickStart, messageBytes)
	_, err := client.ProduceSync(ctx, &message).First()
	if err != nil {
		Log.Error(fmt.Sprintln(err))
		panic(err)
	}
	latencyEnd := time.Now()
	latency := latencyEnd.Sub(latencyStart)
	arrLatency = append(arrLatency, latency)

	time.Sleep(time.Duration(interval) * time.Millisecond)
}

// Produce Message per Second
func produceRatePerSecond(client *kgo.Client, ctx context.Context, ratePerSecond int, quickStart string, messageBytes int, ratepersecondJitter float64) {
	waitStart := time.Now()
	index := 0
	ratePerSecond = makeRatePerSecondJitter(ratePerSecond, ratepersecondJitter)
	for {
		index++
		// Set Data
		message := makeMessage(quickStart, messageBytes)

		// Produce
		latencyStart := time.Now()
		_, err := client.ProduceSync(ctx, &message).First()
		if err != nil {
			Log.Error(fmt.Sprintln(err))
			panic(err)
		}
		latencyEnd := time.Now()
		latency := latencyEnd.Sub(latencyStart)
		arrLatency = append(arrLatency, latency)

		if index%ratePerSecond == 0 {
			waitEnd := time.Now()
			duration := waitEnd.Sub(waitStart)
			if duration < time.Second {
				time.Sleep(time.Second - duration)
			}
			waitStart = time.Now()
			break
		}
	}
}

// Produce Limit Per Second
func produceLimitPerSecond(client *kgo.Client, ctx context.Context, limitPerSecond int) {
	var bytesSent int
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			bytesSent = 0
		default:
			if bytesSent < limitPerSecond {
				valueData := make([]byte, limitPerSecond/100)
				_, err := rand.Read(valueData)
				if err != nil {
					Log.Error(fmt.Sprintln(err))
				}
				byteData, err := json.Marshal(valueData)
				if err != nil {
					fmt.Println(err)
				}

				record := kgo.Record{
					Value: byteData,
				}

				latencyStart := time.Now()
				_, err = client.ProduceSync(ctx, &record).First()
				if err != nil {
					panic(err)
				}
				bytesSent += len(record.Key) + len(record.Value)
				latencyEnd := time.Now()
				latency := latencyEnd.Sub(latencyStart)
				arrLatency = append(arrLatency, latency)
				break
			}
		}
	}
}
