package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli"
)

type ComplexJSON struct {
	Field [100][]string
}

type Value struct {
	TenantID string      `json:"tenent_id"`
	ID       string      `json:"id"`
	Data     ComplexJSON `json:"data"`
}

type Key struct {
	CompanyID string `json:"company_id"`
	ID        string `json:"id"`
}

func randomStringArray() []string {
	rand.Seed(time.Now().UnixNano())
	length := rand.Intn(10)
	var arr []string
	for i := 0; i < length; i++ {
		arr = append(arr, uuid.New().String())
	}
	return arr
}

func main() {
	app := cli.NewApp()
	app.Name = "Workload Generator"
	app.Usage = "Generate workload and send to Kafka"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "brokers, b",
			Value: "localhost:9092",
			Usage: "Bootstrap brokers to connect to",
		},
	}

	app.Action = func(c *cli.Context) error {
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": c.String("brokers")})
		if err != nil {
			panic(err)
		}

		defer p.Close()

		go func() {
			for e := range p.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
					} else {
						fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
					}
				}
			}
		}()

		companyIds := make([]string, 100)
		for i := range companyIds {
			companyIds[i] = uuid.New().String()
		}

		employeeIds := make([]string, 1000)
		for i := range employeeIds {
			employeeIds[i] = uuid.New().String()
		}

		recordsCount := int64(100000000)
		bar := progressbar.Default(recordsCount)
		topic := "rw_qw_customer"
		for i := int64(0); i < recordsCount; i++ {
			var complexData ComplexJSON
			for j := 0; j < 100; j++ {
				complexData.Field[j] = randomStringArray()
			}

			value := Value{
				TenantID: uuid.New().String(),
				ID:       uuid.New().String(),
				Data:     complexData,
			}

			valueBytes, err := json.Marshal(value)
			if err != nil {
				fmt.Printf("json.Marshal error: %s", err)
				continue
			}

			rand.Seed(time.Now().UnixNano())
			companyID := companyIds[rand.Intn(100)]
			id := employeeIds[rand.Intn(1000)]

			key := Key{
				CompanyID: companyID,
				ID:        id,
			}

			keyBytes, err := json.Marshal(key)
			if err != nil {
				fmt.Printf("json.Marshal error: %s", err)
				continue
			}

			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          valueBytes,
				Key:            keyBytes,
			}, nil)
			if err != nil {
				log.Println("Failed to produce message: ", err)
			}

			// Flush every 5000 messages
			if i%5000 == 0 {
				p.Flush(15 * 1000)
			}

			_ = bar.Add(1)
		}
		_ = bar.Finish()
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}
}