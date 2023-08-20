package com.rpl.rama.kafka;

import com.rpl.rama.ops.RamaFunction2;
import org.apache.kafka.clients.producer.*;

import java.util.concurrent.CompletableFuture;

public class KafkaAppend implements RamaFunction2<KafkaExternalDepot, ProducerRecord, CompletableFuture>  {
  boolean _completeWithAck;

  public KafkaAppend() {
    this(true);
  }

  public KafkaAppend(boolean completeWithAck) {
    _completeWithAck = completeWithAck;
  }

  @Override
  public CompletableFuture invoke(KafkaExternalDepot kd, ProducerRecord record) {
    KafkaProducer producer = kd.getProducer();
    final CompletableFuture ret = new CompletableFuture();
    if(_completeWithAck) {
      producer.send(record, (RecordMetadata metadata, Exception e) -> {
        if(e == null) {
          ret.complete(metadata);
        } else {
          ret.completeExceptionally(e);
        }
      });
    } else {
      producer.send(record);
      ret.complete(null);
    }
    return ret;
  }
}
