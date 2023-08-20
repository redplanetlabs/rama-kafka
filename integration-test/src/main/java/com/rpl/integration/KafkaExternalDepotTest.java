package com.rpl.integration;

import com.rpl.rama.*;
import com.rpl.rama.kafka.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.*;

public class KafkaExternalDepotTest {
  private static Map<String, Object> getKafkaConfig() {
    Map<String, Object> ret = new HashMap<>();
    ret.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    ret.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    ret.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    ret.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
    ret.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
    ret.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);
    return ret;
  }

  public static class KafkaExternalDepotModule implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareObject("*kafka", new KafkaExternalDepot(getKafkaConfig(), "test-topic"));
      setup.declareObject("*kafka2", new KafkaExternalDepot(getKafkaConfig(), "test-topic2"));

      StreamTopology s = topologies.stream("s");
      s.source("*kafka", StreamSourceOptions.startFromBeginning()).out("*tuple")
       .each(Ops.EXPAND, "*tuple").out("*k", "*v")
       .shufflePartition()
       .each((String k, Long v) -> new ProducerRecord("test-topic3", k, v * 8),
             "*k", "*v").out("*producerRecord")
       .eachAsync(new KafkaAppend(), "*kafka", "*producerRecord");

      MicrobatchTopology mb1 = topologies.microbatch("mb1");
      mb1.pstate("$$p1", PState.mapSchema(String.class, Long.class));
      mb1.source("*kafka2", MicrobatchSourceOptions.startFromBeginning()).out("*mb")
         .explodeMicrobatch("*mb").out("*tuple")
         .each(Ops.EXPAND, "*tuple").out("*k", "*v")
         .hashPartition("*k")
         .compoundAgg("$$p1", CompoundAgg.map("*k", Agg.sum("*v")));

      MicrobatchTopology mb2 = topologies.microbatch("mb2");
      mb2.pstate("$$p2", PState.mapSchema(String.class, Long.class));
      mb2.source("*kafka", MicrobatchSourceOptions.startFromOffsetAfterTimestamp(32)).out("*mb")
         .explodeMicrobatch("*mb").out("*tuple")
         .each(Ops.EXPAND, "*tuple").out("*k", "*v")
         .hashPartition("*k")
         .compoundAgg("$$p2", CompoundAgg.map("*k", Agg.sum("*v")));

      MicrobatchTopology mb3 = topologies.microbatch("mb3");
      mb3.pstate("$$p3", PState.mapSchema(String.class, Long.class));
      mb3.source("*kafka", MicrobatchSourceOptions.startFromOffsetAfterTimestamp(10000000)).out("*mb")
         .explodeMicrobatch("*mb").out("*tuple")
         .each(Ops.EXPAND, "*tuple").out("*k", "*v")
         .hashPartition("*k")
         .compoundAgg("$$p3", CompoundAgg.map("*k", Agg.sum("*v")));

      MicrobatchTopology mb4 = topologies.microbatch("mb4");
      mb4.pstate("$$p4", PState.mapSchema(String.class, Long.class));
      mb4.source("*kafka").out("*mb")
         .explodeMicrobatch("*mb").out("*tuple")
         .each(Ops.EXPAND, "*tuple").out("*k", "*v")
         .hashPartition("*k")
         .compoundAgg("$$p4", CompoundAgg.map("*k", Agg.sum("*v")));
    }
  }

  private static void publishRecord(KafkaProducer producer, String topic, String k, Long v, long timestamp) throws Exception {
    Random r = new Random();
    producer.send(new ProducerRecord(topic, r.nextInt(8), timestamp, k, v)).get();
  }

  private static List fetchRecords(KafkaConsumer consumer, int numRecords) {
    List ret = new ArrayList();
    long startTime = System.currentTimeMillis();
    while(ret.size() < numRecords) {
      if(System.currentTimeMillis() - startTime > 60000) {
        throw new RuntimeException("Timeout fetching " + ret);
      }
      ConsumerRecords records = consumer.poll(Duration.ofMillis(2000));
      for(ConsumerRecord r: (Iterable<ConsumerRecord>) records) {
        ret.add(Arrays.asList(r.key(), r.value()));
      }
    }
    if(ret.size() != numRecords) {
      throw new RuntimeException("Unexpected number of records " + ret);
    }
    return ret;
  }

  private static void assertEquals(Object expected, Object val) {
    if(expected==null && val != null || expected!=null && !expected.equals(val)) {
      throw new RuntimeException("assertEquals failure " + expected + " vs " + val);
    }
  }

  public static void main(String [] args) throws Exception {
    Map<String, Object> kc = getKafkaConfig();
    kc.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
    kc.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    KafkaProducer producer = new KafkaProducer(kc);
    KafkaConsumer consumer = new KafkaConsumer(kc);
    boolean error = false;
    try {
      consumer.subscribe(Collections.singletonList("test-topic3"));

      System.out.println("Publishing initial records...");
      for(long i = 0; i < 20; i++) {
        publishRecord(producer, "test-topic", "a", i, i * 10);
        publishRecord(producer, "test-topic", "b", i * 10, i * 10 + 5);
      }

      for(long i = 0; i < 20; i++) {
        publishRecord(producer, "test-topic2", "c", i, i * 10);
        publishRecord(producer, "test-topic2", "d", i * 10, i * 10 + 5);
      }

      System.out.println("Launching cluster...");
      try(InProcessCluster cluster = InProcessCluster.create()) {
        RamaModule module = new KafkaExternalDepotModule();
        String moduleName = module.getClass().getName();
        System.out.println("Launching module...");
        cluster.launchModule(module, new LaunchConfig(4, 2));
        PState p1 = cluster.clusterPState(moduleName, "$$p1");
        PState p2 = cluster.clusterPState(moduleName, "$$p2");
        PState p3 = cluster.clusterPState(moduleName, "$$p3");
        PState p4 = cluster.clusterPState(moduleName, "$$p4");

        System.out.println("Waiting for mb1 to process 40 records...");
        cluster.waitForMicrobatchProcessedCount(moduleName, "mb1", 40);
        assertEquals(190L, p1.selectOne(Path.key("c")));
        assertEquals(1900L, p1.selectOne(Path.key("d")));
        assertEquals(null, p1.selectOne(Path.key("a")));
        assertEquals(null, p1.selectOne(Path.key("b")));

        System.out.println("Waiting for mb2 to process 33 records...");
        cluster.waitForMicrobatchProcessedCount(moduleName, "mb2", 33);
        assertEquals(184L, p2.selectOne(Path.key("a")));
        assertEquals(1870L, p2.selectOne(Path.key("b")));
        assertEquals(null, p2.selectOne(Path.key("c")));
        assertEquals(null, p2.selectOne(Path.key("d")));

        System.out.println("Fetching 40 records from test-topic3...");
        List records = fetchRecords(consumer, 40);

        Set expected = new HashSet();
        for(long i = 0; i < 20; i++) {
          expected.add(Arrays.asList("a", i * 8));
          expected.add(Arrays.asList("b", i * 10 * 8));
        }

        System.out.println("Asserting received expected records from test-topic3...");
        assertEquals(expected, new HashSet(records));


        System.out.println("Publishing more records to test-topic...");
        publishRecord(producer, "test-topic", "a", 9L, 100000);
        publishRecord(producer, "test-topic", "b", 7L, 100000);
        publishRecord(producer, "test-topic", "a", 3L, 100000);

        System.out.println("Waiting for mb2 to process 36 records...");
        cluster.waitForMicrobatchProcessedCount(moduleName, "mb2", 36);
        assertEquals(196L, p2.selectOne(Path.key("a")));
        assertEquals(1877L, p2.selectOne(Path.key("b")));
        assertEquals(null, p2.selectOne(Path.key("c")));
        assertEquals(null, p2.selectOne(Path.key("d")));

        System.out.println("Waiting for mb3 to process 3 records...");
        cluster.waitForMicrobatchProcessedCount(moduleName, "mb3", 3);
        assertEquals(12L, p3.selectOne(Path.key("a")));
        assertEquals(7L, p3.selectOne(Path.key("b")));
        assertEquals(null, p3.selectOne(Path.key("c")));
        assertEquals(null, p3.selectOne(Path.key("d")));

        System.out.println("Waiting for mb4 to process 3 records...");
        cluster.waitForMicrobatchProcessedCount(moduleName, "mb4", 3);
        assertEquals(12L, p4.selectOne(Path.key("a")));
        assertEquals(7L, p4.selectOne(Path.key("b")));
        assertEquals(null, p4.selectOne(Path.key("c")));
        assertEquals(null, p4.selectOne(Path.key("d")));

        System.out.println("Done!");
      }
    } catch(Throwable t) {
      error = true;
      t.printStackTrace();
    } finally {
      producer.close();
      consumer.close();
      Runtime.getRuntime().halt(error ? 1 : 0);
    }
  }
}
