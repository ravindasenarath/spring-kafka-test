package com.ravinda.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.retry.policy.AlwaysRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfiguration {

  private static final Logger logger = LoggerFactory.getLogger(KafkaConfiguration.class);
  private Integer concurrency;

  KafkaConfiguration(@Value("${spring.kafka.listener.concurrency}") Integer concurrency) {
    this.concurrency = concurrency;
  }

//   Sender configs
  @Bean
  public KafkaTemplate<String, String> vehicleUpdateSender(
      KafkaProperties kafkaProperties) {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());

    var producerFactory = new DefaultKafkaProducerFactory<String, String>(props);
    return new KafkaTemplate<>(producerFactory);
  }

  // Listener configs
  @Bean
  public ConcurrentKafkaListenerContainerFactory schedulingListenerFactory(
      KafkaProperties kafkaProperties) {
    return this.listenerFactoryHelper(
        kafkaProperties, new StringDeserializer(), new StringDeserializer());
  }

  private <K, V> ConcurrentKafkaListenerContainerFactory<K, V> listenerFactoryHelper(
      KafkaProperties kafkaProperties,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    var retryTemplate = new RetryTemplate();
    var retryPolicy = new AlwaysRetryPolicy();
    retryTemplate.setRetryPolicy(retryPolicy);
    retryTemplate.setBackOffPolicy(new FixedBackOffPolicy());
    retryTemplate.registerListener(new LoggingRetryListener());

    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());

    var kafkaConsumerFactory =
        new DefaultKafkaConsumerFactory<>(props, keyDeserializer, valueDeserializer);

    var factory = new ConcurrentKafkaListenerContainerFactory<K, V>();
    factory.setConsumerFactory(kafkaConsumerFactory);
    factory.getContainerProperties().setAckOnError(false);
    factory.setErrorHandler(new SeekToCurrentErrorHandler());
    factory.setStatefulRetry(true);
    factory.setRetryTemplate(retryTemplate);
    factory.setConcurrency(concurrency);

    return factory;
  }

  private static class LoggingRetryListener extends RetryListenerSupport {

    @Override
    public <T, E extends Throwable> void onError(
            RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
      logger.info("Retrying message, current retry count {}", context.getRetryCount());
    }
  }
}
