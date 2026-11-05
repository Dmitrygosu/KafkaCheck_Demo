package main

import (
	"github.com/Dmitrygosu/KafkaCheck_Demo/KafkaCheck/internal/app"
	"log"
	"os"
)

func main() {
	if err := app.Run(); err != nil {
		log.Printf("application error: %v", err)
		os.Exit(1)
	}
}
