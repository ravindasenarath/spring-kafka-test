package com.ravinda.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TickProducer implements Closeable {
  private static final String TOPIC = "my-test-topic-upstream";

  private final Producer<String, String> producer;

  public TickProducer(String brokers) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    producer = new KafkaProducer<>(props);
  }

  public RecordMetadata publishTick(String brand)
          throws ExecutionException, InterruptedException {
    return publish(TOPIC, brand, Instant.now().toString());
  }

  private RecordMetadata publish(String topic, String key, String value)
      throws ExecutionException, InterruptedException {
    final RecordMetadata recordMetadata;
    recordMetadata = producer.send(new ProducerRecord<>(topic, key, value)).get();
    producer.flush();
    return recordMetadata;
  }

  @PreDestroy
  @Override
  public void close() {
    producer.close();
  }
}
