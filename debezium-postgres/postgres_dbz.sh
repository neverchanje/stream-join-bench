#!/usr/bin/env bash

echo "Deploying Debezium Postgres connector"

curl -s -X PUT -H "Content-Type: application/json" http://127.0.0.1:8083/connectors/register-postgres/config \
  -d '{
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": 5432,
    "database.user": "postgresuser",
    "database.password": "postgres123",
    "database.dbname": "mydb",
    "database.server.name": "postgres",
    "database.schema": "public",
    "database.history.kafka.bootstrap.servers": "0.0.0.0:9092",
    "database.history.kafka.topic": "postgres-history",
    "time.precision.mode": "connect",
    "include.schema.changes": false
}'