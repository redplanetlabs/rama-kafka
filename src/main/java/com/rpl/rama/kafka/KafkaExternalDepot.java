package com.rpl.rama.kafka;

import com.rpl.rama.integration.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import com.rpl.rama.ops.RamaFunction0;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

public class KafkaExternalDepot implements ExternalDepot {
  static final ConcurrentHashMap<List, KafkaConsumerResources> allConsumerResources = new ConcurrentHashMap<>();
  static final ConcurrentHashMap<List, KafkaProducer> allProducers = new ConcurrentHashMap<>();

  Map<String, Object> _kafkaConfig;
  String _topic;
  long _pollTimeoutMillis;

  boolean _isConsumerOwner = false;
  boolean _isProducerOwner = false;
  TaskGlobalContext _context;
  KafkaConsumerResources _kafkaResources;
  KafkaProducer _producer;

  private static class KafkaConsumerResources {
    public ExecutorService executorService;
    public KafkaConsumer consumer;

    public KafkaConsumerResources(Map<String, Object> kafkaConfig) {
      Map<String, Object> c = new HashMap<>();
      for(String k: kafkaConfig.keySet()) {
        c.put(k, kafkaConfig.get(k));
      }
      c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
      c.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
      this.executorService = new ScheduledThreadPoolExecutor(1);
      this.consumer = new KafkaConsumer(c);
    }
  }

  private CompletableFuture runOnKafkaThread(RamaFunction0 fn) {
    final CompletableFuture ret = new CompletableFuture();
    _kafkaResources.executorService.submit(() -> {
      try {
        ret.complete(fn.invoke());
      } catch(Throwable t) {
        ret.completeExceptionally(t);
      }
    });
    return ret;
  }

  public KafkaExternalDepot(Map<String, Object> kafkaConfig, String topic, long pollTimeoutMillis) {
    if(kafkaConfig.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) ||
       kafkaConfig.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
      throw new RuntimeException("KafkaExternalDepot config cannot contain enable.auto.commit or group.id " + kafkaConfig);
    }
    if(!kafkaConfig.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      throw new RuntimeException("KafkaExternalDepot config must contain boostrap.servers");
    }
    _kafkaConfig = kafkaConfig;
    _topic = topic;
    _pollTimeoutMillis = pollTimeoutMillis;
  }

  public KafkaExternalDepot(Map<String, Object> kafkaConfig, String topic) {
    this(kafkaConfig, topic, 2000);
  }

  private static List consumerIdTuple(Map<String, Object> kafkaConfig, TaskGlobalContext context) {
    int taskThreadId = Collections.min(context.getTaskGroup());
    return Arrays.asList(
      context.getModuleInstanceInfo().getModuleInstanceId(),
      taskThreadId,
      kafkaConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
  }

  private static List producerIdTuple(Map<String, Object> kafkaConfig, TaskGlobalContext context) {
    int taskThreadId = Collections.min(context.getTaskGroup());
    return Arrays.asList(
      context.getModuleInstanceInfo().getModuleInstanceId(),
      kafkaConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
  }

  public KafkaProducer getProducer() {
    if(_producer == null) {
      List idTuple = producerIdTuple(_kafkaConfig, _context);
      _producer = allProducers.get(idTuple);
      if(_producer == null) {
        synchronized(allProducers) {
          _producer = allProducers.get(idTuple);
          if(_producer == null) {
            _producer = new KafkaProducer(_kafkaConfig);
            allProducers.put(idTuple, _producer);
            _isProducerOwner = true;
          }
        }
      }
    }
    return _producer;
  }

  @Override
  public void prepareForTask(int taskId, TaskGlobalContext context) {
    _context = context;
    List tuple = consumerIdTuple(_kafkaConfig, context);
    // this logic ensures that even if there are multiple KafkaExternalDepot defined for
    // the module with the same bootstrap config, there will be only one
    // KafkaConsumer / thread for each task thread to handle all of them
    if (allConsumerResources.containsKey(tuple)) {
      _kafkaResources = allConsumerResources.get(tuple);
    } else {
      _isConsumerOwner = true;
      _kafkaResources = new KafkaConsumerResources(_kafkaConfig);
      allConsumerResources.put(tuple, _kafkaResources);
    }
  }

  @Override
  public void close() {
    if(_isConsumerOwner) {
      allConsumerResources.remove(consumerIdTuple(_kafkaConfig, _context));
      _kafkaResources.executorService.shutdown();
      _kafkaResources.consumer.close();
    }
    if(_isProducerOwner) {
      allProducers.remove(producerIdTuple(_kafkaConfig, _context));
      _producer.close();
    }
  }

  @Override
  public CompletableFuture<Integer> getNumPartitions() {
    return runOnKafkaThread(
      () -> _kafkaResources.consumer.partitionsFor(_topic).size());
  }

  @Override
  public CompletableFuture<Long> startOffset(int partitionIndex) {
    return runOnKafkaThread(
      () -> {
        TopicPartition tp = new TopicPartition(_topic, partitionIndex);
        return _kafkaResources.consumer
                              .beginningOffsets(Collections.singletonList(tp))
                              .get(tp);
      });
  }

  @Override
  public CompletableFuture<Long> endOffset(int partitionIndex) {
    return runOnKafkaThread(
      () -> {
        TopicPartition tp = new TopicPartition(_topic, partitionIndex);
        return _kafkaResources.consumer
                              .endOffsets(Collections.singletonList(tp))
                              .get(tp);
      });
  }

  @Override
  public CompletableFuture<Long> offsetAfterTimestampMillis(int partitionIndex, long millis) {
    return runOnKafkaThread(
      () -> {
        TopicPartition tp = new TopicPartition(_topic, partitionIndex);
        OffsetAndTimestamp ot = (OffsetAndTimestamp) _kafkaResources.consumer
                                                                    .offsetsForTimes(Collections.singletonMap(tp, millis))
                                                                    .get(tp);
        if(ot != null) {
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
    for(ConsumerRecord record : (Iterable<ConsumerRecord>) records) {
      if(maybeEndOffset==null || record.offset() < maybeEndOffset) {
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
        while(ret.size() < targetSize) {
          int startSize = ret.size();
          fetchInto(
            ret,
            _kafkaResources.consumer,
            _topic,
            partitionIndex,
            _pollTimeoutMillis,
            startOffset + startSize,
            endOffset);
          if(ret.size() == startSize) {
            throw new RuntimeException("Failed to fetch from Kafka within timeout");
          }
        }
        // this shouldn't be possible
        if(ret.size() != targetSize) {
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
          _kafkaResources.consumer,
          _topic,
          partitionIndex,
          _pollTimeoutMillis,
          startOffset,
          null);
        return ret;
      });
  }
}
