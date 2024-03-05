package src

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzap"
)

// produce type
const (
	PRODUCE_INTERVAL                  = 0
	PRODUCE_RATE_PER_SEC              = 1
	PRODUCE_LIMIT_DATA_AMOUNT_PER_SEC = 2
)

func Datagen() {

	// default values
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
	if Module.Producer.ClientId != "" {
		opts = append(opts, kgo.ClientID(Module.Producer.ClientId))
		Log.Info(fmt.Sprintln("cleint id : ", Module.Producer.ClientId))
	}
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
	jitter := DEFAUTL_JITTER_RATE
	if Module.Datagen.Jitter != "" {
		jitter = stringToFloat64(Module.Datagen.Jitter)
	}

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

	defer client.Close()

	// producer client
	opts = append(opts, kgo.MaxBufferedRecords(250<<20/messageBytes+1))

	for i := 1; i <= workThread; i++ {
		go worker(opts, produceJobs, &wg, ctx, produceType, ratePerSecond, interval, quickstart, messageBytes, limitPerSecond, jitter)
	}

	go func() {
		for {
			produceJobs <- 1
		}
	}()

	wg.Wait()
}

// Producer thread
func worker(opts []kgo.Opt, jobs <-chan int, wg *sync.WaitGroup, ctx context.Context, produceType int, ratePerSecond int, interval int, quickstart string, messageBytes int, limitPerSecond int, jitter float64) {

	// Producer Client
	producerClient, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}

	// Produce Messages
	for range jobs {
		switch produceType {
		case PRODUCE_INTERVAL:
			produceInterval(producerClient, ctx, interval, quickstart, messageBytes, jitter)
		case PRODUCE_RATE_PER_SEC:
			produceRatePerSecond(producerClient, ctx, ratePerSecond, quickstart, messageBytes, jitter)
		case PRODUCE_LIMIT_DATA_AMOUNT_PER_SEC:
			produceLimitPerSecond(producerClient, ctx, limitPerSecond, quickstart, messageBytes, jitter)
		default:
			Log.Info(fmt.Sprintln("the value is missing or invalid in the produce type"))
		}
	}
	// Flush
	if err := producerClient.Flush(ctx); err != nil {
		Log.Error(fmt.Sprintln(err))
		panic(err)
	}
	wg.Done()
}

// Interval Producer
func produceInterval(client *kgo.Client, ctx context.Context, interval int, quickStart string, messageBytes int, jitter float64) {
	interval = makeRatePerSecondJitter(PRODUCE_INTERVAL, interval, jitter)
	message := makeMessage(quickStart, messageBytes)

	latencyStart := time.Now()
	client.Produce(ctx, message, func(r *kgo.Record, err error) {
		if err != nil {
			Log.Error(fmt.Sprintln(err))
			panic(err)
		}
	})
	elapsed := time.Since(latencyStart)
	checkElapsedLatency(elapsed)

	time.Sleep(time.Duration(interval) * time.Millisecond)
}

// Produce Message per Second
func produceRatePerSecond(client *kgo.Client, ctx context.Context, ratePerSecond int, quickStart string, messageBytes int, jitter float64) {
	waitStart := time.Now()
	index := 0
	ratePerSecond = makeRatePerSecondJitter(PRODUCE_RATE_PER_SEC, ratePerSecond, jitter)
	for {
		index++
		// Set Data
		message := makeMessage(quickStart, messageBytes)

		// Produce
		latencyStart := time.Now()
		client.Produce(ctx, message, func(r *kgo.Record, err error) {
			if err != nil {
				Log.Error(fmt.Sprintln(err))
				panic(err)
			}
		})
		elapsed := time.Since(latencyStart)
		checkElapsedLatency(elapsed)

		if index%ratePerSecond == 0 {
			waitEnd := time.Now()
			duration := waitEnd.Sub(waitStart)
			if duration < time.Second {
				time.Sleep(time.Second - duration)
			}
			break
		}
	}
}

// Produce Limit Per Second
func produceLimitPerSecond(client *kgo.Client, ctx context.Context, limitPerSecond int, quickStart string, messageBytes int, jitter float64) {
	var bytesSent int
	ticker := time.NewTicker(time.Second)
	jitterLimitPerSecond := limitPerSecond
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			bytesSent = 0
			jitterLimitPerSecond = makeRatePerSecondJitter(PRODUCE_LIMIT_DATA_AMOUNT_PER_SEC, limitPerSecond, jitter)
		default:
			if bytesSent < jitterLimitPerSecond {
				message := makeMessage(quickStart, messageBytes)
				latencyStart := time.Now()
				client.Produce(ctx, message, func(r *kgo.Record, err error) {
					if err != nil {
						Log.Error(fmt.Sprintln(err))
						panic(err)
					}
				})
				bytesSent += len(message.Key) + len(message.Value)
				elapsed := time.Since(latencyStart)
				checkElapsedLatency(elapsed)
			}
		}
	}
}
