package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"go.uber.org/ratelimit"
	"gopkg.in/yaml.v2"
)

const (
	defaultHost        = "127.0.0.1"
	defaultPort        = 5432
	defaultDB          = "postgres"
	defaultBatchSize   = 50
	defaultThreadCount = 2
)

type Column struct {
	Type      string `yaml:"type"`
	Reference string `yaml:"reference"`

	referenceTable *Table
}

type Table struct {
	Name             string             `yaml:"name"`
	Schema           map[string]*Column `yaml:"schema"`
	UpdateProportion float64            `yaml:"update_proportion"`
	PrimaryKey       string             `yaml:"primary_key"`
	PreloadCount     int64              `yaml:"preload_count"`
	OpsRate          int                `yaml:"operations_per_second"`
	OperationCount   int64              `yaml:"operation_count"`

	// For the convenience of ordered iteration over columns.
	columnNames []string

	insertedCount atomic.Int64
	updatedCount  int64

	bar *mpb.Bar
	wg  *sync.WaitGroup
}

type WorkloadConfig struct {
	Tables []*Table `yaml:"tables"`
}

type testRunner struct {
	wg             sync.WaitGroup
	progress       *mpb.Progress
	workloadConfig *WorkloadConfig
}

func newTestRunner(workloadConfig *WorkloadConfig, preload bool) *testRunner {
	r := &testRunner{}

	p := mpb.New(
		mpb.WithWaitGroup(&r.wg),
	)
	r.wg.Add(len(workloadConfig.Tables))
	r.progress = p
	r.workloadConfig = workloadConfig
	r.rewriteWorkloadConfig(preload)
	return r
}

// Restructure the internal workload configuration to make later operations easier.
func (r *testRunner) rewriteWorkloadConfig(preload bool) {
	tableMap := make(map[string]*Table)
	for _, table := range r.workloadConfig.Tables {
		tableMap[table.Name] = table
	}
	for _, table := range r.workloadConfig.Tables {
		table.columnNames = make([]string, 0)
		for columnName, column := range table.Schema {
			table.columnNames = append(table.columnNames, columnName)

			if column.Reference != "" {
				referenceTable := tableMap[column.Reference]
				if referenceTable == nil {
					log.Fatalf("invalid reference table %s", column.Reference)
					return
				}
				column.referenceTable = referenceTable
				column.Type = "bigint"
			}
		}

		total := table.PreloadCount
		if !preload {
			total = table.OperationCount
		}
		table.bar = r.progress.AddBar(total,
			mpb.PrependDecorators(
				decor.Name(table.Name),
				decor.OnComplete(
					decor.Counters(0, " %d / %d"),
					" done",
				),
			),
			mpb.AppendDecorators(
				decor.AverageSpeed(0, "%.1f Ops/sec"),
			))
		table.wg = &r.wg
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	app := cli.NewApp()
	app.Name = "benchmark-generator"
	app.Usage = "Generate benchmark tables and records in PostgreSQL"
	flags := []cli.Flag{
		&cli.StringFlag{
			Name:  "config-path, c",
			Usage: "Path to the YAML configuration file",
		},
		&cli.StringFlag{
			Name:  "host",
			Value: defaultHost,
			Usage: "Database host",
		},
		&cli.IntFlag{
			Name:  "port",
			Value: defaultPort,
			Usage: "Database port",
		},
		&cli.StringFlag{
			Name:  "database",
			Value: defaultDB,
			Usage: "Database name",
		},
		&cli.StringFlag{
			Name:     "username, u",
			Usage:    "Database username",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "password",
			Usage:    "Database password",
			Value:    "",
			Required: false,
		},
	}
	preloadFlags := append(flags,
		&cli.Int64Flag{
			Name:  "batch",
			Value: defaultBatchSize,
			Usage: "Batch size for ingestion",
		}, &cli.Int64Flag{
			Name:  "thread",
			Value: defaultThreadCount,
			Usage: "The parallelism of ingestions that will load >= 10-million records. Larger parallelism doesn't always mean faster.",
		},
	)
	app.Commands = []cli.Command{
		{
			Name:    "preload",
			Usage:   "Preload data into the database",
			Action:  preloadAction,
			Aliases: []string{"p"},
			Flags:   preloadFlags,
		},
		{
			Name:    "run",
			Usage:   "Run the benchmark",
			Action:  runAction,
			Aliases: []string{"r"},
			Flags:   flags,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func runWorkload(c *cli.Context, preload bool) error {
	configPath := c.String("config-path")
	if configPath == "" {
		return fmt.Errorf("missing --config-path parameter")
	}

	host := c.String("host")
	port := c.Int("port")
	dbName := c.String("database")
	username := c.String("username")
	password := c.String("password")

	// Load the workload configuration from YAML file
	workloadConfig, err := loadWorkloadConfigFromFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to load workload configuration: %s", err)
	}

	// Configure database connection pool
	config, err := pgxpool.ParseConfig(fmt.Sprintf("postgresql://%s:%s@%s:%d/%s",
		username, password, host, port, dbName))
	if err != nil {
		return fmt.Errorf("failed to parse database configuration: %s", err)
	}

	// Create connection pool
	connPool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %s", err)
	}
	defer connPool.Close()

	runner := newTestRunner(&workloadConfig, preload)
	if preload {
		return runner.preloadTables(connPool, c)
	} else {
		return runner.runTables(connPool)
	}
}

// preloadAction is the action for the "preload" command
func preloadAction(c *cli.Context) error {
	return runWorkload(c, true)
}

// runAction is the action for the "run" command
func runAction(c *cli.Context) error {
	return runWorkload(c, false)
}

// loadWorkloadConfigFromFile loads the workload configuration from a YAML file
func loadWorkloadConfigFromFile(filename string) (WorkloadConfig, error) {
	var config WorkloadConfig

	yamlFile, err := os.ReadFile(filename)
	if err != nil {
		return config, err
	}

	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return config, err
	}

	return config, nil
}

// createTable creates a table in the database with the specified columns and types
func createTable(conn *pgxpool.Pool, table *Table) error {
	columnDefinitions := ""
	for columnName, column := range table.Schema {
		if !validateColumnType(column.Type) {
			return fmt.Errorf("column type '%s' (table '%s', column '%s') is not suppported", column.Type, table.Name, columnName)
		}
		columnDefinitions += fmt.Sprintf("%s %s, ", columnName, column.Type)
	}

	primaryKeyDefinition := fmt.Sprintf("%s bigserial PRIMARY KEY", table.PrimaryKey)

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s%s)", table.Name, columnDefinitions, primaryKeyDefinition)

	_, err := conn.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %s", err, query)
	}
	return nil
}

func (r *testRunner) preloadTables(connPool *pgxpool.Pool, c *cli.Context) error {
	batchSize := c.Int64("batch")
	threadCount := c.Int64("thread")

	for _, table := range r.workloadConfig.Tables {
		if err := createTable(connPool, table); err != nil {
			return err
		}
		// Execute the preload sequentially.
		table.preloadRecords(connPool, batchSize, threadCount)
	}
	// wait for passed wg and for all bars to complete and flush
	r.progress.Wait()
	return nil
}

// preloadRecords generates and inserts records into the specified table
func (t *Table) preloadRecords(conn *pgxpool.Pool, batchSize int64, threadCount int64) {
	if t.PreloadCount <= 10000000 {
		preloadRecordsPartition(t, conn, batchSize, 0, t.PreloadCount)
	} else {
		recordsPerPartition := t.PreloadCount / threadCount
		if t.PreloadCount%threadCount != 0 {
			recordsPerPartition++
		}

		var wg sync.WaitGroup
		for i := int64(0); i < threadCount; i++ {
			start := i * recordsPerPartition
			end := start + recordsPerPartition
			if end > t.PreloadCount {
				end = t.PreloadCount
			}

			wg.Add(1)
			go func(start, end int64) {
				defer wg.Done()
				preloadRecordsPartition(t, conn, batchSize, start, end)
			}(start, end)
		}

		wg.Wait()
	}
	t.wg.Done() // End the progress bar.
	log.Printf("Finished. Inserted %d records to table %s", t.PreloadCount, t.Name)
}

func preloadRecordsPartition(t *Table, conn *pgxpool.Pool, batchSize int64, start, end int64) {
	// creating a buffer to hold a batch of records
	valuesBuffer := make([][]interface{}, 0, batchSize)

	for i := start; i < end; i++ {
		valuesBuffer = append(valuesBuffer, t.generateRandomRecordValues())

		// insert when buffer is full or when it's the last record
		if len(valuesBuffer) == int(batchSize) || i == end-1 {
			insertQuery := generateInsertQuery(t, len(valuesBuffer[0]), len(valuesBuffer))
			// flatten the values
			flatValues := flatten(valuesBuffer)
			_, err := conn.Exec(context.Background(), insertQuery, flatValues...)
			if err != nil {
				log.Fatalf("failed to insert records: %s, %s, %s", err, insertQuery, flatValues)
			}
			// reset the buffer
			valuesBuffer = valuesBuffer[:0]
		}

		// update the progress bar
		t.bar.Increment()
	}
}

// runTables runs in parallel for each tabls
func (r *testRunner) runTables(connPool *pgxpool.Pool) error {
	var wg sync.WaitGroup
	for _, table := range r.workloadConfig.Tables {
		wg.Add(1)
		go func(t *Table) {
			t.runOperations(connPool)
			wg.Done()
		}(table)
	}
	wg.Wait()

	// wait for passed wg and for all bars to complete and flush
	r.progress.Wait()

	return nil
}

// runOperations generates both inserts and updates to the table.
// The execution is single-threaded, as the throughput for each table is assumed to be low in the real-world.
// We don't pursue peak performance in this test, we just want to generate a realistic workload and see if the system can sustain the performance.
func (t *Table) runOperations(conn *pgxpool.Pool) {
	insertQuery := generateInsertQuery(t, len(t.columnNames), 1)
	updateQuery := generateUpdateQuery(t)

	// Create a new rate limiter
	var limiter ratelimit.Limiter = nil
	if t.OpsRate > 0 {
		limiter = ratelimit.New(t.OpsRate)
	}

	for i := int64(0); i < t.OperationCount; i++ {
		// Wait for the limiter
		if limiter != nil {
			limiter.Take()
		}

		if rand.Float64() < t.UpdateProportion {
			values := t.generateRandomRecordValues()
			// The last value is for the primary key.
			// Updates can only apply to existing records, so we randomly pick a primary key value between [0, PreloadCount + InsertCount]
			values = append(values, rand.Int63n(t.PreloadCount+t.insertedCount.Load()))
			_, err := conn.Exec(context.Background(), updateQuery, values...)
			if err != nil {
				log.Fatalf("failed to update: %s, %s, %s", err, updateQuery, values)
			}
			t.updatedCount++
		} else {
			values := t.generateRandomRecordValues()
			_, err := conn.Exec(context.Background(), insertQuery, values...)
			if err != nil {
				log.Fatalf("failed to insert record: %s, %s, %s", err, insertQuery, values)
			}
			t.insertedCount.Add(1)
		}
		// update the progress bar
		t.bar.Increment()
	}
	t.wg.Done() // End the progress bar.
	log.Printf("Finished. Performed %d updates and %d inserts to table %s", t.updatedCount, t.insertedCount.Load(), t.Name)
}

// generateInsertQuery generates the INSERT query for the specified table
func generateInsertQuery(table *Table, numFields int, numRecords int) string {
	valuePlaceholders := make([]string, 0, numFields*numRecords)

	for i := 0; i < numRecords; i++ {
		recordPlaceholders := make([]string, 0, numFields)
		for j := 0; j < numFields; j++ {
			// calculating the correct placeholder index
			placeholderIndex := i*numFields + j + 1
			recordPlaceholders = append(recordPlaceholders, fmt.Sprintf("$%d", placeholderIndex))
		}
		valuePlaceholders = append(valuePlaceholders, fmt.Sprintf("(%s)", strings.Join(recordPlaceholders, ", ")))
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table.Name, join(table.columnNames, ", "), strings.Join(valuePlaceholders, ", "))
}

// generateUpdateQuery generates the UPDATE query for the specified table
func generateUpdateQuery(table *Table) string {
	setClauses := make([]string, len(table.Schema))

	for i, columnName := range table.columnNames {
		setClauses[i] = fmt.Sprintf("%s = $%d", columnName, i+1)
	}

	primaryKeyCondition := fmt.Sprintf("%s = $%d", table.PrimaryKey, len(table.Schema)+1)

	return fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		table.Name,
		join(setClauses, ", "),
		primaryKeyCondition)
}

// generateRandomRecordValues generates random values for each column in the table
func (t *Table) generateRandomRecordValues() []interface{} {
	values := make([]interface{}, len(t.Schema))

	for i, columnName := range t.columnNames {
		column := t.Schema[columnName]
		if column.Reference != "" {
			// If the column is a foreign key, we need to pick a value from the referenced table
			values[i] = rand.Int63n(t.PreloadCount + t.insertedCount.Load())
			continue
		}

		values[i] = generateRandomValueForType(column.Type)
	}

	return values
}

// generateRandomValueForType generates a random value for the specified column type
func generateRandomValueForType(columnType string) interface{} {
	switch columnType {
	case "integer":
		return rand.Intn(100000)
	case "bigint":
		return rand.Int63n(100000000)
	case "varchar":
		return generateRandomString(10)
	case "text":
		return generateRandomString(20)
	case "boolean":
		return rand.Intn(2) == 1
	case "numeric":
		return rand.Float64() * 100
	case "date":
		return time.Now().AddDate(0, 0, rand.Intn(30))
	case "timestamp":
		return time.Now().Add(time.Duration(rand.Intn(60)) * time.Minute)
	case "jsonb":
		return map[string]interface{}{
			"key1": generateRandomString(5),
			"key2": rand.Intn(100),
			"key3": generateRandomString(8),
		}
	default:
		log.Panicf("column type %s is not supported", columnType)
		return nil
	}
}

// validateColumnType validates that the specified column type is supported
func validateColumnType(columnType string) bool {
	validTypes := []string{"integer", "bigint", "varchar", "text", "boolean", "numeric", "date", "timestamp", "jsonb", "bigserial"}
	for _, validType := range validTypes {
		if columnType == validType {
			return true
		}
	}
	return false
}

// generateRandomString generates a random string of the specified length
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// join concatenates the elements of a string slice using a separator
func join(elements []string, separator string) string {
	return strings.Join(elements, separator)
}

// flatten flattens a 2D slice into a 1D slice
func flatten(values [][]interface{}) []interface{} {
	var flat []interface{}
	for _, value := range values {
		flat = append(flat, value...)
	}
	return flat
}
