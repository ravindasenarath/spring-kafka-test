package com.ravinda.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.ExecutionException;

@ExtendWith(BootstrapExtension.class)
@SpringBootTest
@EnableCaching
@ActiveProfiles("test")

// Mark context as dirty in order to close kafka streams after test
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class AbstractSpringBootTest {
  protected static final int DEFAULT_TIMEOUT_SECONDS = 25;

  private TickProducer tickProducer;

  @BeforeEach
  public void before() {
    tickProducer =
            new TickProducer(System.getProperty(EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS));
  }

  protected RecordMetadata publishTick(String brand)
          throws ExecutionException, InterruptedException {
    return tickProducer.publishTick(brand);
  }
}
