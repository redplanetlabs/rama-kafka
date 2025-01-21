package com.rpl.rama.kafka;

import com.rpl.rama.integration.*;

import java.io.IOException;
import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import com.rpl.rama.ops.RamaFunction0;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

public class KafkaExternalDepot implements ExternalDepot {
  static ConcurrentHashMap<List, KafkaConsumerResources> CONSUMERS = new ConcurrentHashMap();
  WorkerManagedResource<KafkaProducer> _producer;

  Map<String, Object> _kafkaConfig;
  String _topic;
  long _pollTimeoutMillis;
  Integer _numPartitions;
  List _consumerId;
  KafkaConsumerResources _consumer;

  private static class KafkaConsumerResources implements Closeable {
    public ExecutorService executorService;
    public KafkaConsumer consumer;
    int count = 1;

    public KafkaConsumerResources(Map<String, Object> kafkaConfig) {
      Map<String, Object> c = new HashMap<>();
      for (String k : kafkaConfig.keySet()) {
        c.put(k, kafkaConfig.get(k));
      }
      c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
      c.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
      this.executorService = new ScheduledThreadPoolExecutor(1);
      this.consumer = new KafkaConsumer(c);
    }

    public void close() throws IOException {
      executorService.shutdown();
      consumer.close();
    }
  }

  private CompletableFuture runOnKafkaThread(RamaFunction0 fn) {
    final CompletableFuture ret = new CompletableFuture();
    _consumer.executorService.submit(() -> {
      try {
        ret.complete(fn.invoke());
      } catch (Throwable t) {
        ret.completeExceptionally(t);
      }
    });
    return ret;
  }

  public KafkaExternalDepot(Map<String, Object> kafkaConfig, String topic, long pollTimeoutMillis) {
    if (kafkaConfig.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) ||
        kafkaConfig.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
      throw new RuntimeException(
          "KafkaExternalDepot config cannot contain enable.auto.commit or group.id " + kafkaConfig);
    }
    if (!kafkaConfig.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      throw new RuntimeException("KafkaExternalDepot config must contain boostrap.servers");
    }
    _kafkaConfig = kafkaConfig;
    _topic = topic;
    _pollTimeoutMillis = pollTimeoutMillis;
  }

  public KafkaExternalDepot(Map<String, Object> kafkaConfig, String topic) {
    this(kafkaConfig, topic, 2000);
  }

  public KafkaExternalDepot staticNumPartitions(int numPartitions) {
    _numPartitions = numPartitions;
    return this;
  }

  private static List consumerIdTuple(Map<String, Object> kafkaConfig, TaskGlobalContext context) {
    int taskThreadId = Collections.min(context.getTaskGroup());
    return Arrays.asList(
        taskThreadId,
        kafkaConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
  }

  public KafkaProducer getProducer() {
    return _producer.getResource();
  }

  @Override
  public void prepareForTask(int taskId, TaskGlobalContext context) {
    _consumerId = consumerIdTuple(_kafkaConfig, context);
    synchronized(CONSUMERS) {
      if(CONSUMERS.contains(_consumerId)) {
        _consumer = CONSUMERS.get(_consumerId);
        _consumer.count++;
      } else {
        _consumer = new KafkaConsumerResources(_kafkaConfig);
        CONSUMERS.put(_consumerId, _consumer);
      }
    }
    _producer = new WorkerManagedResource("producer", context, () -> new KafkaProducer(_kafkaConfig));
  }

  @Override
  public void close() throws IOException {
    synchronized(CONSUMERS) {
      _consumer.count--;
      if(_consumer.count==0) {
        _consumer.close();
        CONSUMERS.remove(_consumerId);
      }
    }
    _producer.close();
  }

  @Override
  public CompletableFuture<Integer> getNumPartitions() {
    if(_numPartitions!=null) {
      return CompletableFuture.completedFuture(_numPartitions);
    } else {
      return runOnKafkaThread(
          () -> _consumer.consumer.partitionsFor(_topic).size());
    }
  }

  @Override
  public CompletableFuture<Long> startOffset(int partitionIndex) {
    return runOnKafkaThread(
        () -> {
          TopicPartition tp = new TopicPartition(_topic, partitionIndex);
          return _consumer.consumer
              .beginningOffsets(Collections.singletonList(tp))
              .get(tp);
        });
  }

  @Override
  public CompletableFuture<Long> endOffset(int partitionIndex) {
    return runOnKafkaThread(
        () -> {
          TopicPartition tp = new TopicPartition(_topic, partitionIndex);
          return _consumer.consumer
              .endOffsets(Collections.singletonList(tp))
              .get(tp);
        });
  }

  @Override
  public CompletableFuture<Long> offsetAfterTimestampMillis(int partitionIndex, long millis) {
    return runOnKafkaThread(
        () -> {
          TopicPartition tp = new TopicPartition(_topic, partitionIndex);
          OffsetAndTimestamp ot = (OffsetAndTimestamp) _consumer.consumer
              .offsetsForTimes(Collections.singletonMap(tp, millis))
              .get(tp);
          if (ot != null) {
            return ot.offset();
          } else {
            return null;
          }
        });
  }

  private static void fetchInto(
      List ret,
      KafkaConsumer consumer,
      String topic,
      int partitionIndex,
      long pollTimeoutMillis,
      long startOffset,
      Long maybeEndOffset) {
    TopicPartition tp = new TopicPartition(topic, partitionIndex);
    consumer.assign(Collections.singletonList(tp));
    consumer.seek(tp, startOffset);
    ConsumerRecords records = consumer.poll(Duration.ofMillis(pollTimeoutMillis));
    for (ConsumerRecord record : (Iterable<ConsumerRecord>) records) {
      if (maybeEndOffset == null || record.offset() < maybeEndOffset) {
        ret.add(Arrays.asList(record.key(), record.value()));
      }
    }
  }

  @Override
  public CompletableFuture<List> fetchFrom(int partitionIndex, long startOffset, long endOffset) {
    return runOnKafkaThread(
        () -> {
          List ret = new ArrayList();
          long targetSize = endOffset - startOffset;
          while (ret.size() < targetSize) {
            int startSize = ret.size();
            fetchInto(
                ret,
                _consumer.consumer,
                _topic,
                partitionIndex,
                _pollTimeoutMillis,
                startOffset + startSize,
                endOffset);
            if (ret.size() == startSize) {
              TopicPartition tp = new TopicPartition(_topic, partitionIndex);
              Long actualStartoffset = (Long) _consumer.consumer
                                                           .beginningOffsets(Collections.singletonList(tp))
                                                           .get(tp);
              throw new RuntimeException("Failed to fetch from Kafka within timeout of " + _pollTimeoutMillis + "ms, fetched " + startSize + " records total, target size " + targetSize + ", start offset " + startOffset + ", end offset " + endOffset + ", topic " + _topic + ", partition index " + partitionIndex + ", actual start offset " + actualStartoffset);
            }
          }
          // this shouldn't be possible
          if (ret.size() != targetSize) {
            throw new RuntimeException(
                "fetchFrom unexpectedly fetched wrong amount of data " +
                    ret.size() + " vs. " + targetSize);
          }
          return ret;
        });
  }

  @Override
  public CompletableFuture<List> fetchFrom(int partitionIndex, long startOffset) {
    return runOnKafkaThread(
        () -> {
          List ret = new ArrayList();
          fetchInto(
              ret,
              _consumer.consumer,
              _topic,
              partitionIndex,
              _pollTimeoutMillis,
              startOffset,
              null);
          return ret;
        });
  }
}
