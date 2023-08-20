#!/usr/bin/env bash

set -eo pipefail

KAFKA_VERSION="kafka_2.13-3.2.1"
KAFKA_FILE="$KAFKA_VERSION.tgz"

if [ ! -f $KAFKA_FILE ]
then
  wget https://archive.apache.org/dist/kafka/3.2.1/$KAFKA_FILE
fi

rm -rf $KAFKA_VERSION
rm -rf /tmp/kafka-logs
rm -rf /tmp/zookeeper
tar xzf $KAFKA_FILE
cd $KAFKA_VERSION
echo "Launching Zookeeper..."
bin/zookeeper-server-start.sh config/zookeeper.properties > ZK_LOG &
echo "Launching Kafka..."
bin/kafka-server-start.sh config/server.properties > KAFKA_LOG &
echo "Creating topics..."
bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 8 --config "message.timestamp.type=CreateTime" --config "retention.ms=-1"
bin/kafka-topics.sh --create --topic test-topic2 --bootstrap-server localhost:9092 --partitions 8 --config "message.timestamp.type=CreateTime" --config "retention.ms=-1"
bin/kafka-topics.sh --create --topic test-topic3 --bootstrap-server localhost:9092 --partitions 1

function finish {
  ps ax | grep kafka.Kafka | awk '{print $1}' | xargs kill -9 || :
  ps ax | grep zookeeper.server | awk '{print $1}' | xargs kill -9 || :
}
trap finish EXIT

cd ..
echo "Running test..."
mvn compile
mvn exec:java@Run
