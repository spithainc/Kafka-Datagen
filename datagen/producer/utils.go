package producer

import (
	"context"
	"fmt"
	"spitha/datagen/datagen/config"
	"spitha/datagen/datagen/logger"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
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
func checkTopic(ctx context.Context, opts []kgo.Opt, ct config.TopicConfig) error {
	var adminClient *kadm.Client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer client.Close()
	adminClient = kadm.NewClient(client)
	topicList, err := adminClient.ListTopics(ctx, ct.Name)
	if err != nil {
		return err
	}

	// no topic
	if !topicList.Has(ct.Name) {
		partition := int32(3)     // default partition
		replicafactor := int16(1) // default replicafactor
		if ct.Partition != "" {
			partition = int32(stringToInt(ct.Partition))
		}
		if ct.Replicafactor != "" {
			replicafactor = int16(stringToInt(ct.Replicafactor))
		}
		_, err := adminClient.CreateTopic(ctx, partition, replicafactor, nil, ct.Name)
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
**                            Type utils                             **
**                                                                   **
***********************************************************************/
func stringToInt(value string) int {
	i, err := strconv.Atoi(value)
	if err != nil {
		logger.Log.Error(fmt.Sprintln(err))
		return 0
	}
	return i
}

func stringToFloat64(value string) float64 {

	i, err := strconv.ParseFloat(value, 64)
	if err != nil {
		logger.Log.Error(fmt.Sprintln(err))
		return 0
	}
	return i
}

/**********************************************************************
**                                                                   **
**                            Metric print                           **
**                                                                   **
***********************************************************************/
func metricTicker(ticker *time.Ticker) {
	for range ticker.C {
		if MessageNumber != 0 {
			logger.Log.Info(fmt.Sprintln("min latency : ", MinLatency))
			logger.Log.Info(fmt.Sprintln("max latency : ", MaxLatency))
			logger.Log.Info(fmt.Sprintln("avg latency : ", TotalTime/time.Duration(MessageNumber)))
			logger.Log.Info(fmt.Sprintln("number messages : ", MessageNumber))
			fmt.Println()

			// init values
			MinLatency = time.Duration(int64(1<<63 - 1))
			MaxLatency = 0
			AvgLatency = 0
			MessageNumber = 0
			TotalTime = 0
		} else {
			logger.Log.Info(fmt.Sprintln("number messages : ", 0))
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
