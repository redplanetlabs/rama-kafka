# rama-kafka

This library integrates Rama with external [Apache Kafka](https://kafka.apache.org/) clusters. It enables Kafka to be used as a source for Rama topologies and makes it easy to publish records to Kafka topics from topologies.

## Maven

`rama-kafka` is available in the following Nexus repository:

```
<repository>
  <id>rpl-releases</id>
  <url>https://nexus.redplanetlabs.com/repository/maven-releases</url>
</repository>
```

The latest release is:

```
<dependency>
  <groupId>com.rpl</groupId>
  <artifactId>rama-kafka</artifactId>
  <version>0.9.0</version>
</dependency>
```

## Usage

General information about integrating Rama with external systems can be found [on this page](https://redplanetlabs.com/docs/~/integrating.html).

Here's an example of using `rama-kafka` to consume from one Kafka topic and publish to another Kafka topic:

```java
public class KafkaIntegrationExampleModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    Map<String, Object> kafkaConfig = new HashMap<>();
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1.mycompany.com:9092,kafka2.mycompany.com:9092");
    kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
    kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
    setup.declareObject("*kafka", new KafkaExternalDepot(kafkaConfig, "myTopic"));

    StreamTopology s = topologies.stream("s");
    s.source("*kafka").out("*tuple")
     .each(Ops.EXPAND, "*tuple").out("*key", "*value")
     .each((String k, Long v) -> new ProducerRecord("anotherTopic", k, v * 8),
           "*key", "*value").out("*producerRecord")
     .eachAsync(new KafkaAppend(), "*kafka", "*producerRecord");
  }
}
```

A topic on a remote Kafka cluster is declared in a Rama module by creating a `KafkaExternalDepot` and passing it to a `declareObject` call. In this example the topic `"myTopic"` on a Kafka cluster is associated with the var `"*kafka"`. `KafkaExternalDepot` is parameterized with a map of configs, the same way you would create a [KafkaConsumer](https://javadoc.io/static/org.apache.kafka/kafka-clients/3.2.3/org/apache/kafka/clients/consumer/KafkaConsumer.html) or [KafkaProducer](https://javadoc.io/static/org.apache.kafka/kafka-clients/3.2.3/org/apache/kafka/clients/producer/KafkaProducer.html). The configs accepted are the same.

`KafkaExternalDepot` uses the `"bootstrap.servers"` config to identify a Kafka cluster. Internally it will only make one `KafkaConsumer` client per task thread per Kafka cluster. So if you are consuming multiple Kafka topics from the same cluster in the same module, as long as each `KafkaExternalDepot` is declared with the same `"bootstrap.servers"` config only one `KafkaConsumer` will be created for that cluster per task thread.

The configs `"enable.auto.commit"` and `"group.id"` are not allowed to be specified as configs because Rama handles all offset management.

Serialization/deserialization to and from a Kafka topic is handled by Kafka, and you can specify the serializations to use via the config object.

When used as a source for a topology, a `KafkaExternalDepot` emits two-element lists containing the key and value of each record. As shown here, `Ops.EXPAND` is useful for extracting the key and value from each emitted list.  There's no difference in functionality between using `KafkaExternalDepot` as a source versus a built-in depot (e.g. all "start from" options are supported).

### Publishing to a Kafka topic

The `KafkaAppend` function publishes records to a Kafka topic from within a topology. It takes as input a `KafkaExternalDepot` and [ProducerRecord](https://javadoc.io/static/org.apache.kafka/kafka-clients/3.2.3/org/apache/kafka/clients/producer/ProducerRecord.html). The `ProducerRecord` contains the topic and partition information for publishing as well as the key and value of the record.

`KafkaAppend` should be used with `eachAsync` and emits the [RecordMetadata](https://javadoc.io/static/org.apache.kafka/kafka-clients/3.2.3/org/apache/kafka/clients/producer/RecordMetadata.html) returned by Kafka.

By default `KafkaAppend` in an `eachAsync` doesn't emit until Kafka has returned the `RecordMetadata`. This ties success of the topology doing the append with success of the record being published to Kafka. So if the Kafka append fails, the topology will retry. If you don't care about this and prefer an at-most once append guarantee, you can parameterize the `KafkaAppend` constructor to tell it to emit without waiting to hear back from Kafka. For example:

```java
.eachAsync(new KafkaAppend(false), "*kafka", "*producerRecord")
```

In this case the `eachAsync` call will emit `null` and the topology will succeed without waiting for Kafka to acknowledge the append.
