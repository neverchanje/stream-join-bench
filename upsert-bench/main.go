package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/Shopify/sarama"
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
	ID        string `json:"_id"`
}

func randomStringArray() []string {
	var arr []string
	size := rand.Intn(17) // Generate strings of size 0 to 16 bytes
	arr = append(arr, randomString(size))
	return arr
}

// randomString generates a random string of the given length.
func randomString(length int) string {
	chars := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	var b []rune
	for i := 0; i < length; i++ {
		b = append(b, chars[rand.Intn(len(chars))])
	}
	return string(b)
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
		rand.Seed(time.Now().UnixNano())

		config := sarama.NewConfig()
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Return.Successes = true
		config.Producer.Compression = sarama.CompressionSnappy
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
		defer func() {
			_ = bar.Finish()
		}()
		go func() {
			for range producer.Successes() {
				_ = bar.Add(1)
			}
		}()

		companyIds := make([]string, 100)
		for i := range companyIds {
			companyIds[i] = randomString(4)
		}

		for i := 0; i < recordsCount; i++ {
			var complexData ComplexJSON
			for j := 0; j < 100; j++ {
				complexData.Field[j] = randomStringArray()
			}

			value := Value{
				TenantID: randomString(12),
				ID:       randomString(36),
				Data:     complexData,
			}

			valueBytes, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("json.Marshal error: %s", err)
			}

			companyID := companyIds[rand.Intn(100)]
			id := rand.Intn(1000000)

			key := Key{
				CompanyID: companyID,
				ID:        fmt.Sprintf("%d", id),
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
