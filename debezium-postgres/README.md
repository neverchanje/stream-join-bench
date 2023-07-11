# Data Preparation

1. Start the Kafka+Postgres+Debezium cluster.
    ```sh
    docker compose up -d
    ```

2. Preload the dataset into Postgres. The Kafka will still be empty at this point.
    ```sh
    cd ..
    ./main p --config-path ./2-way-non-pk-join.yml --host localhost --username postgresuser --password postgres123 --database mydb
    ```

3. Asynchronously synchroinize the Postgres data into Kafka.
    ```sh
    ./debezium-postgres/postgres_dbz.sh
    ```
