#!/bin/bash

eval "$(pyenv init -)"
pyenv activate RTstreaming
# Start Docker Compose
docker-compose up -d

# Wait for the services to be fully up and running
sleep 300

# Run spark-submit with the specified packages and configurations
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --conf spark.cassandra.connection.host=localhost spark_stream.py