package datagen

import (
	"spitha/datagen/datagen/config"
	"spitha/datagen/datagen/logger"
	"spitha/datagen/datagen/producer"
)

/**********************************************************************
**                                                                   **
**                               Handler                             **
**                                                                   **
***********************************************************************/
func Handler(configPath string) {

	// Init logger
	logger.InitLogger()

	// Init config
	config := config.InitConfig(configPath)

	// datagen
	producer.Datagen(config)
}
