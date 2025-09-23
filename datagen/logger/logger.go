package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var Log *zap.Logger

func InitLogger() {
	// Init Log
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
	Log = zap.New(core, zap.AddCallerSkip(1))
	defer Log.Sync()
}
