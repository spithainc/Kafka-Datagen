package src

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/plugin/kzap"
)

// produce, message type, quickstart
const (
	PRODUCE_TYPE_INTERVAL                  = 0
	PRODUCE_TYPE_RATE_PER_SEC              = 1
	PRODUCE_TYPE_LIMIT_DATA_AMOUNT_PER_SEC = 2

	MESSAGE_TYPE_QUICKSTART    = 0
	MESSAGE_TYPE_MESSAGE_BYTES = 1

	QUICKSTART_USER    = "user"
	QUICKSTART_BOOK    = "book"
	QUICKSTART_CAR     = "car"
	QUICKSTART_ADDRESS = "address"
	QUICKSTART_CONTACT = "contact"
	QUICKSTART_MOVIE   = "movie"
	QUICKSTART_JOB     = "job"
)

type ProduceSettings struct {
	Jitter          float64
	ProduceType     uint8
	ProduceSettings struct {
		Interval                 int
		RatePerSecond            int
		LimitDataAmountPerSecond int
	}
	MessageType     uint8
	MessageSettings struct {
		Quickstart   string
		MessageBytes int
	}
}

/**********************************************************************
**                                                                   **
**                         Datagen main func                         **
**                                                                   **
***********************************************************************/
func Datagen() {
	var (
		DEFAULT_WORK_TRHEAD   = 1
		DEFAULT_MESSAGE_BYTES = 100
		DEFAULT_INTERVAL      = 0
		DEFAUTL_JITTER_RATE   = 0.0
	)

	ctx := context.Background()
	opts := []kgo.Opt{}

	/*******************************
	**  Producer - Bootstrap server
	********************************/
	bootstrapServer := Module.BootstrapServer
	if bootstrapServer == "" {
		panic("please input producer.bootstrap.servers settings")
	}
	opts = append(opts, kgo.SeedBrokers(strings.Split(bootstrapServer, ",")...))
	opts = append(opts, kgo.WithLogger(kzap.New(Log))) // log
	opts = append(opts, kgo.RequiredAcks(kgo.NoAck())) // ack = 0
	opts = append(opts, kgo.DisableIdempotentWrite())  // idempotence = disable

	/*******************************
	**   Producer - Kafka Auth
	********************************/
	opts, err := auth(opts)
	if err != nil {
		Log.Error(fmt.Sprintln(err))
		panic(err)
	}

	/*******************************
	**   Producer - Client ID
	********************************/
	// proudcer client id
	if Module.Producer.ClientId != "" {
		opts = append(opts, kgo.ClientID(Module.Producer.ClientId))
		Log.Info(fmt.Sprintln("cleint id : ", Module.Producer.ClientId))
	}

	/*******************************
	**   Producer - Message
	********************************/
	// proudcer max message bytes
	if Module.Producer.MaxMessageBytes != "" {
		opts = append(opts, kgo.ProducerBatchMaxBytes(int32(stringToInt(Module.Producer.MaxMessageBytes))))
	}

	// proudcer lingers
	if Module.Producer.Lingers != "" {
		opts = append(opts, kgo.ProducerLinger(time.Duration(stringToInt(Module.Producer.Lingers))))
	}

	/*******************************
	**   Producer - Compression type
	********************************/
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

	/*******************************
	**   Producer - Topic Name
	********************************/
	// setting for topic
	topic := Module.Topic.Name
	if topic == "" {
		panic("please input topic settings")
	}
	opts = append(opts, kgo.DefaultProduceTopic(topic))

	/*******************************
	**   Datagen - Go routine
	********************************/
	// datagen work thread
	workThread := DEFAULT_WORK_TRHEAD
	if Module.Datagen.GoRoutine != "" {
		workThread = stringToInt(Module.Datagen.GoRoutine)
	}

	/*******************************
	**   Datagen - Produce type
	********************************/
	var produceSettings ProduceSettings

	// datagen produce type
	produceSettings.ProduceType = PRODUCE_TYPE_INTERVAL         // default produce type
	produceSettings.ProduceSettings.Interval = DEFAULT_INTERVAL // default interval
	if Module.Datagen.Interval != "" {
		// type interval per message
		produceSettings.ProduceSettings.Interval = stringToInt(Module.Datagen.Interval)
		produceSettings.ProduceType = PRODUCE_TYPE_INTERVAL
	} else if Module.Datagen.RatePerSecond != "" {
		// type rate per second
		produceSettings.ProduceSettings.RatePerSecond = stringToInt(Module.Datagen.RatePerSecond)
		produceSettings.ProduceType = PRODUCE_TYPE_RATE_PER_SEC
	} else if Module.Datagen.LimitDataAmountPerSecond != "" {
		// type limit per second
		produceSettings.ProduceSettings.LimitDataAmountPerSecond = stringToInt(Module.Datagen.LimitDataAmountPerSecond)
		workThread = 1 // limt per second
		produceSettings.ProduceType = PRODUCE_TYPE_LIMIT_DATA_AMOUNT_PER_SEC
	}

	// check data
	Log.Info(fmt.Sprintln("produce type : ", produceSettings.ProduceType))
	if !checkParams(Module.Datagen.Interval, Module.Datagen.RatePerSecond, Module.Datagen.LimitDataAmountPerSecond) {
		panic("Only one of the options (interval, rate-per-second, limit-data-amount-per-second) can be selected.")
	}

	/*******************************
	**   Datagen - Message type
	********************************/
	produceSettings.MessageType = MESSAGE_TYPE_MESSAGE_BYTES             // default message type
	produceSettings.MessageSettings.MessageBytes = DEFAULT_MESSAGE_BYTES // default quickstart

	// quickstart
	if Module.Datagen.QuickStart != "" {
		produceSettings.MessageSettings.Quickstart = Module.Datagen.QuickStart
		produceSettings.MessageType = MESSAGE_TYPE_QUICKSTART
		if Module.Datagen.QuickStart != QUICKSTART_USER && Module.Datagen.QuickStart != QUICKSTART_BOOK && Module.Datagen.QuickStart != QUICKSTART_CAR && Module.Datagen.QuickStart != QUICKSTART_CONTACT && Module.Datagen.QuickStart != QUICKSTART_JOB && Module.Datagen.QuickStart != QUICKSTART_MOVIE && Module.Datagen.QuickStart != QUICKSTART_ADDRESS {
			panic("The quickstart option is limited to the following options: user, book, car, address, contact, movie, and job.")
		}
	}

	// message bytes
	if Module.Datagen.MessageBytes != "" {
		produceSettings.MessageSettings.MessageBytes = stringToInt(Module.Datagen.MessageBytes)
		opts = append(opts, kgo.MaxBufferedRecords(250<<20/produceSettings.MessageSettings.MessageBytes+1))
		produceSettings.MessageType = MESSAGE_TYPE_MESSAGE_BYTES
	}

	// check data
	Log.Info(fmt.Sprintln("message type : ", produceSettings.MessageType))
	if !checkParams(Module.Datagen.MessageBytes, Module.Datagen.QuickStart) {
		panic("Only one of the options (message-bytes, quickstart) can be selected.")
	}

	/*******************************
	**   Datagen - Jitter
	********************************/
	// datagen jitter setting
	produceSettings.Jitter = DEFAUTL_JITTER_RATE
	if Module.Datagen.Jitter != "" {
		produceSettings.Jitter = stringToFloat64(Module.Datagen.Jitter)
	}

	/*******************************
	**   Admin - Check Topic
	********************************/
	err = checkTopic(ctx, opts)
	if err != nil {
		panic(err)
	}

	/*******************************
	**   Schema registry
	********************************/
	var serde sr.Serde
	if Module.Producer.SchemaRegistry.Server.Urls != "" && Module.Datagen.QuickStart != "" { // only quickstart
		Log.Info("use schema registry")
		srOpts := []sr.Opt{}
		srOpts = append(srOpts, sr.URLs(Module.Producer.SchemaRegistry.Server.Urls))

		// basic auth
		if Module.Producer.SchemaRegistry.Server.Username != "" && Module.Producer.SchemaRegistry.Server.Password != "" {
			srOpts = append(srOpts, sr.BasicAuth(Module.Producer.SchemaRegistry.Server.Username, Module.Producer.SchemaRegistry.Server.Password))
		}

		// create client
		schemaRegistryClient, err := sr.NewClient(srOpts...)
		if err != nil {
			panic(err)
		}

		if Module.Producer.SchemaRegistry.Type == "avro" {
			// avro
			// schema template
			var schemaTemplate = `{
					"type": "record",
					"name": "datagen_spitha",
					"namespace": "datagen.spitha.io",
					"fields" : []
				}`
			var schemaSrc interface{}
			switch produceSettings.MessageSettings.Quickstart {
			case QUICKSTART_USER:
				schemaSrc = PersonInfo{}
			case QUICKSTART_BOOK:
				schemaSrc = BookInfo{}
			case QUICKSTART_CAR:
				schemaSrc = CarInfo{}
			case QUICKSTART_ADDRESS:
				schemaSrc = AddressInfo{}
			case QUICKSTART_CONTACT:
				schemaSrc = ContactInfo{}
			case QUICKSTART_MOVIE:
				schemaSrc = MovieInfo{}
			case QUICKSTART_JOB:
				schemaSrc = JobInfo{}
			}

			// add quickstart field
			schema, err := generateAvroSchema(schemaTemplate, schemaSrc)
			if err != nil {
				Log.Error(fmt.Sprintln("Error generating schema:", err))
				panic(err)
			}
			Log.Debug(schema)

			// find schema in schema registry
			schemaRegistrySchema, err := schemaRegistryClient.CreateSchema(context.Background(), Module.Producer.SchemaRegistry.Subject, sr.Schema{
				Schema: schema,
				Type:   sr.TypeAvro,
			})
			if err != nil {
				panic(err)
			}
			Log.Info(fmt.Sprintf("created or reusing schema subject %q version %d id %d\n", schemaRegistrySchema.Subject, schemaRegistrySchema.Version, schemaRegistrySchema.ID))

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
		} else {
			Log.Info("Currently, only the Avro type is supported.")
			os.Exit(1)
		}
	}

	/*******************************
	**   Producer - Go routine
	********************************/
	var wg sync.WaitGroup
	wg.Add(workThread)
	produceJobs := make(chan int, workThread)

	for i := 1; i <= workThread; i++ {
		go worker(opts, produceJobs, &wg, ctx, &produceSettings, &serde)
	}

	go func() {
		for {
			produceJobs <- 1
		}
	}()

	wg.Wait()
}

/**********************************************************************
**                                                                   **
**                        Producer go routine                        **
**                                                                   **
***********************************************************************/
func worker(opts []kgo.Opt, jobs <-chan int, wg *sync.WaitGroup, ctx context.Context, produceSettings *ProduceSettings, serde *sr.Serde) {

	// Producer Client
	producerClient, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}

	// Produce Messages
	for range jobs {
		switch produceSettings.ProduceType {
		case PRODUCE_TYPE_INTERVAL:
			produceInterval(producerClient, ctx, produceSettings, serde)
		case PRODUCE_TYPE_RATE_PER_SEC:
			produceRatePerSecond(producerClient, ctx, produceSettings, serde)
		case PRODUCE_TYPE_LIMIT_DATA_AMOUNT_PER_SEC:
			produceLimitPerSecond(producerClient, ctx, produceSettings, serde)
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

/**********************************************************************
**                                                                   **
**                         Interval Producer                         **
**                                                                   **
***********************************************************************/
func produceInterval(client *kgo.Client, ctx context.Context, produceSettings *ProduceSettings, serde *sr.Serde) {
	interval := makeRatePerSecondJitter(PRODUCE_TYPE_INTERVAL, produceSettings.ProduceSettings.Interval, produceSettings.Jitter)
	message := makeMessage(produceSettings, serde)

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

/**********************************************************************
**                                                                   **
**                   Produce Message per Second                      **
**                                                                   **
***********************************************************************/
func produceRatePerSecond(client *kgo.Client, ctx context.Context, produceSettings *ProduceSettings, serde *sr.Serde) {
	waitStart := time.Now()
	index := 0
	ratePerSecond := makeRatePerSecondJitter(PRODUCE_TYPE_RATE_PER_SEC, produceSettings.ProduceSettings.RatePerSecond, produceSettings.Jitter)
	for {
		index++
		// Set Data
		message := makeMessage(produceSettings, serde)

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

/**********************************************************************
**                                                                   **
**                    Produce Limit Per Second                       **
**                                                                   **
***********************************************************************/
func produceLimitPerSecond(client *kgo.Client, ctx context.Context, produceSettings *ProduceSettings, serde *sr.Serde) {
	var bytesSent int
	ticker := time.NewTicker(time.Second)
	jitterLimitPerSecond := produceSettings.ProduceSettings.LimitDataAmountPerSecond
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			bytesSent = 0
			jitterLimitPerSecond = makeRatePerSecondJitter(PRODUCE_TYPE_LIMIT_DATA_AMOUNT_PER_SEC, produceSettings.ProduceSettings.LimitDataAmountPerSecond, produceSettings.Jitter)
		default:
			if bytesSent < jitterLimitPerSecond {
				message := makeMessage(produceSettings, serde)
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
