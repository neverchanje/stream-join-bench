package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/schollz/progressbar/v3"
	"github.com/segmentio/kafka-go"
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
		writer := kafka.NewWriter(kafka.WriterConfig{
			Brokers:  []string{c.String("brokers")},
			Topic:    "rw_qw_customer",
			Balancer: &kafka.LeastBytes{},
		})
		defer writer.Close()

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

			err = writer.WriteMessages(context.Background(),
				kafka.Message{
					Key:   keyBytes,
					Value: valueBytes,
				},
			)
			if err != nil {
				return fmt.Errorf("Failed to write messages:", err)
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
