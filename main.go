package main

import (
	"flag"
	_ "net/http/pprof"
	"spitha/datagen/datagen"
)

func main() {

	configPath := flag.String("config", "datagen.yaml", "Input config file")
	flag.Parse()

	datagen.Handler(*configPath)
}
