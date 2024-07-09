package src

import (
	"time"

	"go.uber.org/zap"
)

var Log *zap.Logger

/**********************************************************************
**                                                                   **
**                               Handler                             **
**                                                                   **
***********************************************************************/
func Handler() {
	// ticker
	ticker := time.NewTicker(1 * time.Second)
	go metricTicker(ticker)

	// datagen
	Datagen()
}
