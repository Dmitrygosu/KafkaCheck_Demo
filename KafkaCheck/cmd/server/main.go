package main

import (
	"flag"
	"log"

	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/app"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "path to the configuration file")
	flag.Parse()

	if err := app.Run(*configPath); err != nil {
		log.Fatalf("application error: %v", err)
	}
}
