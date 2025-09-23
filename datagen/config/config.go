package config

import (
	"fmt"
	"os"
	"path/filepath"
	"spitha/datagen/datagen/logger"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"github.com/kardianos/osext"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

// var Config *ConfigConfig

/**********************************************************************
**                                                                   **
**                       Read external config                        **
**                                                                   **
***********************************************************************/
// In order to handle empty values, a string format is necessary, since the default value for integers is 0.
type ConfigConfig struct {
	BootstrapServer string         `yaml:"bootstrap-server"`
	Producer        ProducerConfig `yaml:"producer"`
	Topic           TopicConfig    `yaml:"topic"`
	Datagen         DatagenConfig  `yaml:"datagen"`
}

type ProducerConfig struct {
	MaxMessageBytes string `yaml:"max-message-bytes"`
	Lingers         string `yaml:"lingers"`
	CompressionType string `yaml:"compression-type"`
	ClientId        string `yaml:"client-id"`        // producer client-id
	TransactionalID string `yaml:"transactional-id"` // producer transactional-id
	SchemaRegistry  struct {
		Server struct {
			Urls     string `yaml:"urls"`
			Username string `yaml:"username"`
			Password string `yaml:"password"`
		} `yaml:"server"`
		Subject string `yaml:"subject"`
		Type    string `yaml:"type"`
	} `yaml:"schema-registry"`
	Sasl struct {
		Mechanism          string `yaml:"mechanism"` // sasl, plain
		Username           string `yaml:"username"`
		Password           string `yaml:"password"`
		AwsAccessKeyId     string `yaml:"aws-access-key-id"` // aws iam
		AwsSecretAccessKey string `yaml:"aws-secret-access-key"`
		ClientId           string `yaml:"client-id"` // oatuh
		ClientSecret       string `yaml:"client-secret"`
		TokenEndpoint      string `yaml:"token-endpoint"`
		KerberosConfig     string `yaml:"kerberos-config-path"` // kerberos
		KeyTab             string `yaml:"keytab-path"`
		Realm              string `yaml:"realm"`
		Servicename        string `yaml:"servicename"`
	} `yaml:"sasl"`
	Tls struct {
		Certfile   string `yaml:"certfile"`
		Keyfile    string `yaml:"keyfile"`
		Cafile     string `yaml:"cafile"`
		SkipVerify bool   `yaml:"skipverify"`
	} `yaml:"tls"`
}

type TopicConfig struct {
	Name          string `yaml:"name"`
	Partition     string `yaml:"partition"`
	Replicafactor string `yaml:"replica-factor"`
}

type DatagenConfig struct {
	GoRoutine string `yaml:"go-routine"`
	Jitter    string `yaml:"jitter"`
	Proudce   struct {
		Mode             string `yaml:"mode"`
		Interval         string `yaml:"interval"`
		RatePerSecond    string `yaml:"rate-per-second"`
		DataRateLimitBPS string `yaml:"data-rate-limit-bps"`
	} `yaml:"produce"`
	Message struct {
		Mode         string `yaml:"mode"`
		QuickStart   string `yaml:"quickstart"`
		MessageBytes string `yaml:"message-bytes"`
	} `yaml:"message"`
}

func InitConfig(configPath string) *ConfigConfig {
	var config *ConfigConfig
	filename, _ := filepath.Abs(configPath)
	yamlFile, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		logger.Log.Error(err.Error())
		os.Exit(1)
		return nil
	}
	logger.Log.Info("Successfully loaded configuration file")

	// read config
	viper.SetConfigFile(filename)
	readErr := viper.ReadInConfig() // Find and read the config file
	if readErr != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", readErr))
	}

	viper.OnConfigChange(func(e fsnotify.Event) {
		fmt.Fprintln(os.Stderr, "Config file changed: ", e.Name)
		file, err := osext.Executable()
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed retrieving executable name:", err.Error())
			fmt.Fprintln(os.Stderr, "Manual restart is needed")
			return
		}
		err = syscall.Exec(file, os.Args, os.Environ())
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed restarting:", err.Error())
			fmt.Fprintln(os.Stderr, "Manual restart is needed")
			return
		}

	})
	viper.WatchConfig()
	return config
}
