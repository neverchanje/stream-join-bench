package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/Shopify/sarama"
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
		config := sarama.NewConfig()
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Return.Successes = true
		producer, err := sarama.NewAsyncProducer([]string{c.String("brokers")}, config)
		if err != nil {
			return err
		}
		defer producer.AsyncClose()

		go func() {
			for err := range producer.Errors() {
				fmt.Println("Failed to write message:", err)
			}
		}()

		const recordsCount = 100000000
		bar := progressbar.Default(recordsCount)
		defer bar.Finish()
		go func() {
			for range producer.Successes() {
				_ = bar.Add(1)
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

		for i := 0; i < recordsCount; i++ {
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
				return fmt.Errorf("json.Marshal error: %s", err)
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
				return fmt.Errorf("json.Marshal error: %s", err)
			}

			msg := &sarama.ProducerMessage{
				Topic: "rw_qw_customer",
				Key:   sarama.ByteEncoder(keyBytes),
				Value: sarama.ByteEncoder(valueBytes),
			}
			producer.Input() <- msg
		}

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}
}
