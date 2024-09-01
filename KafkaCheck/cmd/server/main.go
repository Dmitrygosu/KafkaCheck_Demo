package server

import (
	"KafkaCheck/internal/kafka"
	"flag"
	"log"
	"strings"
	"time"
)

func main() {
	// Параметры cmdшки
	kafkaBrokers := flag.String("brokers", "localhost:9092", "Kafka brokers (comma-separated)")
	kafkaTopic := flag.String("topic", "test_topic", "Kafka topic")
	flag.Parse()

	brokers := strings.Split(*kafkaBrokers, ",")

	// Создание продюсера
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		log.Fatal("Error creating producer:", err)
	}
	defer producer.Close()

	// Создание консьюмера
	consumer, err := kafka.NewConsumer(brokers)
	if err != nil {
		log.Fatal("Error creating consumer:", err)
	}
	defer consumer.Close()

	// Запуск консьюмера в отдельной горутине(какой важный)
	go func() {
		consumer.ConsumeMessages(*kafkaTopic)
	}()

	// Отправка сообщения в Kafka
	message := "Hello Kafka!"
	producer.SendMessage(*kafkaTopic, nil, []byte(message))

	log.Printf("Message sent: %s", message)

	// Ожидание консьюмера ( ЕСЛИ НЕ ЗАПУСКАЮТСЯ КОНТЕЙНЕРЫ-УВЕЛИЧИТЬ ВРЕМЯ(ДУМАЮ НАДО ФУНКЦИЮ ДЛЯ ЭТОГО НАПИСАТЬ,
	//НИЖЕ НАБРОСОК,думаю нормальные люди тут хелсчек могут добавить)
	time.Sleep(12 * time.Second)

}

//ПРИМЕР
//func waitForKafka(brokers []string) error {
//	baseWaitTime := 2 * time.Second
//	maxWaitTime := 30 * time.Second
//	attempts := 5
//
//	for attempt := 1; attempt <= attempts; attempt++ {
//		allReady := true
//
//		for _, broker := range brokers {
//			conn, err := sarama.Dial("tcp", broker)
//			if err == nil {
//				conn.Close()
//				log.Printf("Брокер Kafka %s готов", broker)
//			} else {
//				log.Printf("Брокер Kafka %s не готов: %v", broker, err)
//				allReady = false
//			}
//		}
//
//		if allReady {
//			return nil
//		}
//
//		waitTime := baseWaitTime * time.Duration(attempt)
//		if waitTime > maxWaitTime {
//			waitTime = maxWaitTime
//		}
//
//		log.Printf("Попробую снова через %v секунд...", waitTime.Seconds())
//		time.Sleep(waitTime)
//	}
//
//	return fmt.Errorf("некоторые брокеры Kafka не готовы после %d попыток", attempts)
//}
//
//func main() {
//	brokers := []string{"localhost:9092", "localhost:9093", "localhost:9094"}
//	if err := waitForKafka(brokers); err != nil {
//		log.Fatalf("Ошибка: %v", err)
//	}
//	log.Println("Все брокеры Kafka готовы")
//}
//}
