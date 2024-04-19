package main

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"spitha/datagen/src"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"github.com/kardianos/osext"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v2"
)

func main() {

	configPath := flag.String("config", "datagen.yaml", "Input config file")
	flag.Parse()

	fmt.Println(*configPath)

	// Init Log
	var err error
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.StacktraceKey = ""

	cores := []zapcore.Core{}
	// Create a core for log file
	fileSyncer := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "logs/datagen.log",
		MaxSize:    10, // megabytes
		MaxBackups: 10,
		MaxAge:     30, // days
	})
	fileCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		fileSyncer,
		zap.InfoLevel,
	)
	cores = append(cores, fileCore)

	// Create a core for stderr
	stderrSyncer := zapcore.AddSync(os.Stderr)
	stderrCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		stderrSyncer,
		zap.InfoLevel,
	)
	cores = append(cores, stderrCore)

	// Combine all cores
	core := zapcore.NewTee(cores...)
	src.Log = zap.New(core, zap.AddCallerSkip(1))
	if err != nil {
		panic(err)
	}
	defer src.Log.Sync()

	// Init config
	filename, _ := filepath.Abs(*configPath)
	yamlFile, err := os.ReadFile(filename)
	err = yaml.Unmarshal(yamlFile, &src.Module)
	if err != nil {
		src.Log.Error(err.Error())
		os.Exit(1)
		return
	}
	src.Log.Info("Successfully loaded configuration file")

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

	src.Handler()
}
