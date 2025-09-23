package producer

import (
	"context"
	"fmt"
	"os"
	"spitha/datagen/datagen/config"
	"spitha/datagen/datagen/logger"
	"spitha/datagen/datagen/message"
	"spitha/datagen/datagen/message/avro"
	"spitha/datagen/datagen/message/protobuf"
	"spitha/datagen/datagen/value"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/plugin/kzap"
)

type datagenProducer struct {
	Jitter  float64
	Produce struct {
		Mode                     string
		Interval                 int
		RatePerSecond            int
		LimitDataAmountPerSecond int
	}
	Message struct {
		Mode         string
		Quickstart   string
		MessageBytes int
	}
	SchemaRegistry struct {
		MessageType string
		Serde       sr.Serde
	}
	Transaction struct {
		Enabled bool
		Id      string
	}
	SRMessageType string
}

/**********************************************************************
**                                                                   **
**                         Datagen main func                         **
**                                                                   **
***********************************************************************/
func Datagen(config *config.ConfigConfig) {

	// ticker
	ticker := time.NewTicker(1 * time.Second)
	go metricTicker(ticker)

	ctx := context.Background()
	opts := []kgo.Opt{}

	/*******************************
	**  Producer - Bootstrap server
	********************************/
	bootstrapServer := config.BootstrapServer
	if bootstrapServer == "" {
		panic("please input producer.bootstrap.servers settings")
	}
	opts = append(opts, kgo.SeedBrokers(strings.Split(bootstrapServer, ",")...))
	opts = append(opts, kgo.WithLogger(kzap.New(logger.Log))) // log

	/*******************************
	**   Producer - Kafka Auth
	********************************/
	opts, err := auth(opts, config.Producer)
	if err != nil {
		logger.Log.Error(fmt.Sprintln(err))
		panic(err)
	}

	/*******************************
	**   Producer - Client ID
	********************************/
	// proudcer client id
	if config.Producer.ClientId != "" {
		opts = append(opts, kgo.ClientID(config.Producer.ClientId))
		logger.Log.Info(fmt.Sprintln("cleint id : ", config.Producer.ClientId))
	}

	/*******************************
	**   Producer - Transactional ID
	********************************/
	// proudcer transactional id
	if config.Producer.TransactionalID == "" {
		opts = append(opts, kgo.DisableIdempotentWrite())  // idempotence = disable
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck())) // ack = 0
	}

	/*******************************
	**   Producer - Message
	********************************/
	// proudcer max message bytes
	if config.Producer.MaxMessageBytes != "" {
		opts = append(opts, kgo.ProducerBatchMaxBytes(int32(stringToInt(config.Producer.MaxMessageBytes))))
	}

	// proudcer lingers
	if config.Producer.Lingers != "" {
		opts = append(opts, kgo.ProducerLinger(time.Duration(stringToInt(config.Producer.Lingers))))
	}

	/*******************************
	**   Producer - Compression type
	********************************/
	if config.Producer.CompressionType != "" {
		switch config.Producer.CompressionType {
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
	topic := config.Topic.Name
	if topic == "" {
		panic("please input topic settings")
	}
	opts = append(opts, kgo.DefaultProduceTopic(topic))

	/*******************************
	**   Datagen - Go routine
	********************************/
	// datagen work thread
	workThread := 1
	if config.Datagen.GoRoutine != "" {
		workThread = stringToInt(config.Datagen.GoRoutine)
	}

	/*******************************
	**   Datagen - Produce mode
	********************************/
	var dp datagenProducer

	// datagen produce mode
	switch config.Datagen.Proudce.Mode {
	case "interval":
		dp.Produce.Mode = value.PRODUCE_MODE_INTERVAL
		if config.Datagen.Proudce.Interval != "" {
			dp.Produce.Interval = stringToInt(config.Datagen.Proudce.Interval)
		} else {
			dp.Produce.Interval = 100
		}
	case "rate-per-second":
		dp.Produce.Mode = value.PRODUCE_MODE_RATE_PER_SEC
		if config.Datagen.Proudce.RatePerSecond != "" {
			dp.Produce.RatePerSecond = stringToInt(config.Datagen.Proudce.RatePerSecond)
		} else {
			dp.Produce.RatePerSecond = 100
		}
	case "data-rate-limit-bps":
		dp.Produce.Mode = value.PRODUCE_MODE_DATA_RATE_LIMIT_BPS
		if config.Datagen.Proudce.DataRateLimitBPS != "" {
			dp.Produce.LimitDataAmountPerSecond = stringToInt(config.Datagen.Proudce.DataRateLimitBPS)
		} else {
			dp.Produce.LimitDataAmountPerSecond = 100
		}
	}

	/*******************************
	**   Datagen - Transaction Producer
	********************************/
	// datagen work thread
	if config.Producer.TransactionalID != "" {
		dp.Transaction.Enabled = true
		dp.Transaction.Id = config.Producer.TransactionalID
	} else {
		dp.Transaction.Enabled = false
	}

	/*******************************
	**   Datagen - Message type
	********************************/
	switch config.Datagen.Message.Mode {
	case value.MESSAGE_MODE_QUICKSTART:
		dp.Message.Mode = value.MESSAGE_MODE_QUICKSTART
		dp.Message.Quickstart = config.Datagen.Message.QuickStart
		if config.Datagen.Message.QuickStart != value.QUICKSTART_USER &&
			config.Datagen.Message.QuickStart != value.QUICKSTART_BOOK &&
			config.Datagen.Message.QuickStart != value.QUICKSTART_CAR &&
			config.Datagen.Message.QuickStart != value.QUICKSTART_CONTACT &&
			config.Datagen.Message.QuickStart != value.QUICKSTART_JOB &&
			config.Datagen.Message.QuickStart != value.QUICKSTART_MOVIE &&
			config.Datagen.Message.QuickStart != value.QUICKSTART_ADDRESS {
			panic("The quickstart option is limited to the following options: user, book, car, address, contact, movie, and job.")
		}
	case value.MESSAGE_MODE_MESSAGE_BYTES:
		dp.Message.Mode = value.MESSAGE_MODE_MESSAGE_BYTES
		if config.Datagen.Message.MessageBytes != "" {
			dp.Message.MessageBytes = stringToInt(config.Datagen.Message.MessageBytes)
		} else {
			dp.Message.MessageBytes = 100
		}
		opts = append(opts, kgo.MaxBufferedRecords(250<<20/dp.Message.MessageBytes+1))
	}

	/*******************************
	**   Datagen - Jitter
	********************************/
	// datagen jitter setting
	dp.Jitter = 0.0
	if config.Datagen.Jitter != "" {
		dp.Jitter = stringToFloat64(config.Datagen.Jitter)
	}

	/*******************************
	**   Admin - Check Topic
	********************************/
	err = checkTopic(ctx, opts, config.Topic)
	if err != nil {
		panic(err)
	}

	/*******************************
	**   Schema registry
	********************************/
	if config.Producer.SchemaRegistry.Server.Urls != "" && config.Datagen.Message.QuickStart != "" { // only quickstart
		dp.SRMessageType = config.Producer.SchemaRegistry.Type
		logger.Log.Info("use schema registry")
		srOpts := []sr.ClientOpt{}
		srOpts = append(srOpts, sr.URLs(config.Producer.SchemaRegistry.Server.Urls))

		// basic auth
		if config.Producer.SchemaRegistry.Server.Username != "" && config.Producer.SchemaRegistry.Server.Password != "" {
			srOpts = append(srOpts, sr.BasicAuth(config.Producer.SchemaRegistry.Server.Username, config.Producer.SchemaRegistry.Server.Password))
		}

		// create client
		srClient, err := sr.NewClient(srOpts...)
		if err != nil {
			panic(err)
		}

		switch config.Producer.SchemaRegistry.Type {
		case "avro":
			avro.HelperAvro(dp.Message.Quickstart, srClient, &dp.SchemaRegistry.Serde, config.Producer.SchemaRegistry.Subject)
		case "protobuf":
			protobuf.HelperProtobuf(dp.Message.Quickstart, srClient, &dp.SchemaRegistry.Serde, config.Producer.SchemaRegistry.Subject)
		default:
			logger.Log.Info("only the (avro, protobuf) type is supported.")
			os.Exit(1)
		}
	}

	/*******************************
	**   Producer - Go routine
	********************************/
	var wg sync.WaitGroup
	wg.Add(workThread)

	for i := 1; i <= workThread; i++ {
		go dp.worker(i, opts, &wg, ctx)
	}

	wg.Wait()
}

/**********************************************************************
**                                                                   **
**                        Producer go routine                        **
**                                                                   **
***********************************************************************/
func (ds *datagenProducer) worker(index int, opts []kgo.Opt, wg *sync.WaitGroup, ctx context.Context) {

	// Producer Transaction
	if ds.Transaction.Enabled {
		transactionId := fmt.Sprintf("%s-%d", ds.Transaction.Id, index)
		opts = append(opts, kgo.TransactionalID(transactionId))
		opts = append(opts, kgo.TransactionTimeout(5*time.Second))
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
		logger.Log.Info(fmt.Sprintln("transactional id : ", transactionId))
	}

	// Producer Client
	producerClient, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer producerClient.Close()

	// Produce Messages
	// for range jobs {
	switch ds.Produce.Mode {
	case value.PRODUCE_MODE_INTERVAL:
		ds.produceInterval(producerClient, ctx)
	case value.PRODUCE_MODE_RATE_PER_SEC:
		ds.produceRatePerSecond(producerClient, ctx)
	case value.PRODUCE_MODE_DATA_RATE_LIMIT_BPS:
		ds.produceLimitPerSecond(producerClient, ctx)
	default:
		logger.Log.Info(fmt.Sprintln("the value is missing or invalid in the produce type"))
	}
	// }
	// Flush
	if err := producerClient.Flush(ctx); err != nil {
		logger.Log.Error(fmt.Sprintln(err))
		panic(err)
	}
	wg.Done()
}

/**********************************************************************
**                                                                   **
**                         Interval Producer                         **
**                                                                   **
***********************************************************************/
func (ds *datagenProducer) produceInterval(client *kgo.Client, ctx context.Context) {
	for {
		// Begin a new transaction if enabled
		if ds.Transaction.Enabled {
			if err := client.BeginTransaction(); err != nil {
				logger.Log.Error(fmt.Sprintln(err))
				continue
			}
		}

		// Compute jittered sleep interval for pacing
		jitterInterval := message.MakeRatePerSecondJitter(ds.Produce.Mode, ds.Produce.Interval, ds.Jitter)

		// Build a record (avoid naming the var "message" to prevent confusion with the package)
		rec := message.MakeMessage(&ds.SchemaRegistry.Serde, ds.Message.Mode, ds.Message.Quickstart, ds.Message.MessageBytes, ds.SRMessageType)

		// Use an atomic flag to signal whether we must abort the transaction
		var needAbort atomic.Bool

		// latency measurement
		latencyStart := time.Now()

		// Asynchronous produce with a callback.
		// IMPORTANT: Do not end/commit/abort the transaction inside this callback.
		client.Produce(ctx, rec, func(r *kgo.Record, err error) {
			if err != nil {
				// Just mark that we need to abort and log the error.
				// Transaction finalization is handled outside the callback.
				needAbort.Store(true)
				logger.Log.Error(fmt.Sprintln(err))
			}
		})

		// latency tracking after scheduling the produce
		elapsed := time.Since(latencyStart)
		checkElapsedLatency(elapsed)

		if ds.Transaction.Enabled {
			// Wait for all in-flight sends + callbacks to finish.
			// Without Flush, some records may still be buffered and not part of this transaction.
			if err := client.Flush(ctx); err != nil {
				// If Flush fails, prefer the abort path.
				needAbort.Store(true)
				logger.Log.Error(fmt.Sprintln(err))
			}

			if needAbort.Load() {
				// Remove any not-yet-sent records so they don't leak into the next transaction
				_ = client.AbortBufferedRecords(ctx)
				// Abort the current transaction
				_ = client.EndTransaction(ctx, kgo.TryAbort)
			} else {
				// Try to commit; if it fails with an abortable state, immediately TryAbort
				if err := client.EndTransaction(ctx, kgo.TryCommit); err != nil {
					logger.Log.Error(fmt.Sprintln(err))
					_ = client.EndTransaction(ctx, kgo.TryAbort)
				}
			}
		}

		// Sleep after the transaction is finalized (commit/abort)
		time.Sleep(time.Duration(jitterInterval) * time.Millisecond)
	}
}

/**********************************************************************
**                                                                   **
**                   Produce Message per Second                      **
**                                                                   **
***********************************************************************/
func (ds *datagenProducer) produceRatePerSecond(client *kgo.Client, ctx context.Context) {
	// Per-second pacing window
	windowStart := time.Now()

	// Initial RPS with jitter; re-evaluated each window
	rps := message.MakeRatePerSecondJitter(value.PRODUCE_MODE_RATE_PER_SEC, ds.Produce.RatePerSecond, ds.Jitter)
	if rps <= 0 {
		rps = 1
	}

	// Tracks whether we're currently inside a transaction
	inTxn := false

	// Ensure we are in a transaction before producing.
	// Returns true if producing is safe; false if we failed to open a transaction.
	ensureTxn := func() bool {
		if !ds.Transaction.Enabled {
			return true
		}
		if inTxn {
			return true
		}
		if err := client.BeginTransaction(); err != nil {
			logger.Log.Error(fmt.Sprintf("begin txn: %q", err))
			// Back off a bit to avoid log storms / tight loops
			time.Sleep(25 * time.Millisecond)
			return false
		}
		inTxn = true
		return true
	}

	// Try to enter a transaction at startup (best-effort)
	_ = ensureTxn()

	sentThisWindow := 0
	var needAbort atomic.Bool // Set by callbacks on any produce error (window-scoped)

	for {
		// 1) Never produce unless we're definitely in a transaction when enabled
		if !ensureTxn() {
			continue
		}

		// 2) Build one record
		rec := message.MakeMessage(&ds.SchemaRegistry.Serde, ds.Message.Mode, ds.Message.Quickstart, ds.Message.MessageBytes, ds.SRMessageType)

		// 3) Async produce; DO NOT end/commit/abort inside the callback.
		start := time.Now()
		client.Produce(ctx, rec, func(r *kgo.Record, err error) {
			if err != nil {
				// Signal that this window must abort
				needAbort.Store(true)
				logger.Log.Error(fmt.Sprintf("produce err: %q", err))
			}
		})
		checkElapsedLatency(time.Since(start))
		sentThisWindow++

		// 4) Window boundary: reached target RPS for this second
		if sentThisWindow >= rps {
			// Finish pacing for this 1s window
			if rem := time.Second - time.Since(windowStart); rem > 0 {
				time.Sleep(rem)
			}

			// 5) End the transaction for this window (commit if clean, else abort),
			//    then (if successful) begin the next transaction right away.
			if ds.Transaction.Enabled && inTxn {
				endTry := kgo.TryCommit
				if needAbort.Load() {
					endTry = kgo.TryAbort
				}

				// End → (maybe) Begin in one call; EndAndBeginTransaction performs a Flush internally.
				err := client.EndAndBeginTransaction(
					ctx,
					kgo.EndBeginTxnSafe, // Safe mode: blocks new produces until end completes
					endTry,              // Commit or Abort based on the window status
					func(ctx context.Context, endErr error) error {
						if endErr != nil {
							// If ending failed, return the error so Begin is skipped.
							logger.Log.Error(fmt.Sprintf("end txn err: %q", endErr))
							return endErr
						}
						return nil // Proceed to BeginTransaction internally
					},
				)

				if err != nil {
					// We failed to end-and-begin; mark ourselves out of a txn.
					// The next loop will call ensureTxn() to re-enter.
					inTxn = false
					// Gentle backoff to avoid hammering the coordinator when unhealthy
					time.Sleep(50 * time.Millisecond)

					// try to explicitly start a new txn now instead of waiting
					if berr := client.BeginTransaction(); berr != nil {
						logger.Log.Error(fmt.Sprintf("begin txn after error: %q", berr))
					} else {
						inTxn = true
					}
				} else {
					// Success: we are already inside the next transaction
					inTxn = true
				}
			}

			// 6) Start the next window
			windowStart = time.Now()
			sentThisWindow = 0
			needAbort.Store(false)

			// Recompute RPS with jitter for the new window
			rps = message.MakeRatePerSecondJitter(value.PRODUCE_MODE_RATE_PER_SEC, ds.Produce.RatePerSecond, ds.Jitter)
			if rps <= 0 {
				rps = 1
			}
		}
	}
}

/**********************************************************************
**                                                                   **
**                    Produce Limit Per Second                       **
**                                                                   **
***********************************************************************/
func (ds *datagenProducer) produceLimitPerSecond(client *kgo.Client, ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	// Initial per-second byte budget (jitter recalculated each tick)
	limitBps := ds.Produce.LimitDataAmountPerSecond
	if limitBps <= 0 {
		limitBps = 1
	}

	var (
		bytesSent int
		inTxn     bool        // whether a txn is open for the current window
		needAbort atomic.Bool // set by callbacks if any produce failed in the window
	)

	// Ensure we are inside a transaction before producing (when transactions are enabled).
	ensureTxn := func() bool {
		if !ds.Transaction.Enabled {
			return true
		}
		if inTxn {
			return true
		}
		if err := client.BeginTransaction(); err != nil {
			logger.Log.Error(fmt.Sprintf("begin txn: %q", err))
			// brief backoff to avoid log storms
			time.Sleep(25 * time.Millisecond)
			return false
		}
		inTxn = true
		return true
	}

	// Try to enter a txn at startup (best-effort)
	_ = ensureTxn()

	for {
		select {
		// ----- window boundary: once per second -----
		case <-ticker.C:
			if ds.Transaction.Enabled && inTxn && bytesSent > 0 {
				// Decide commit vs abort based on callback errors in this window.
				endTry := kgo.TryCommit
				if needAbort.Load() {
					endTry = kgo.TryAbort
				}

				// End → (maybe) Begin in one call; includes Flush internally.
				err := client.EndAndBeginTransaction(
					ctx,
					kgo.EndBeginTxnSafe,
					endTry,
					func(ctx context.Context, endErr error) error {
						if endErr != nil {
							logger.Log.Error(fmt.Sprintf("end txn err: %q", endErr))
							// Return non-nil to skip Begin; we’ll re-enter with ensureTxn() next loop.
							return endErr
						}
						return nil
					},
				)
				if err != nil {
					inTxn = false // not in a txn now; next send will re-open
				} else {
					inTxn = true // new txn already started for the next window
				}
			}
			// Reset window counters/state
			bytesSent = 0
			needAbort.Store(false)

			// Recompute jittered limit for the next second
			limitBps = message.MakeRatePerSecondJitter(value.PRODUCE_MODE_DATA_RATE_LIMIT_BPS, ds.Produce.LimitDataAmountPerSecond, ds.Jitter)
			if limitBps <= 0 {
				limitBps = 1
			}

		default:
			// If we already hit the byte budget, briefly yield until the next tick.
			if bytesSent >= limitBps {
				time.Sleep(300 * time.Microsecond)
				continue
			}

			// Guard: do not produce unless we are inside a transaction (when enabled).
			if !ensureTxn() {
				continue
			}

			// Build one record (avoid variable name "message" to not shadow the package)
			rec := message.MakeMessage(&ds.SchemaRegistry.Serde, ds.Message.Mode, ds.Message.Quickstart, ds.Message.MessageBytes, ds.SRMessageType)

			// Async produce; never end/commit/abort a txn inside this callback.
			start := time.Now()
			client.Produce(ctx, rec, func(r *kgo.Record, err error) {
				if err != nil {
					needAbort.Store(true)
					logger.Log.Error(fmt.Sprintf("produce err: %q", err))
				}
			})
			checkElapsedLatency(time.Since(start))

			// Update simple byte accounting (key + value).
			// If you want to include headers/overhead, add them here.
			bytesSent += len(rec.Key) + len(rec.Value)
		}
	}
}
