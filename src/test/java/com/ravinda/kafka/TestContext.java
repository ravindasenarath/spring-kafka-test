package com.ravinda.kafka;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.HashMap;
import java.util.Map;

public class TestContext implements AutoCloseable, ExtensionContext.Store.CloseableResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestContext.class);

  private final EmbeddedKafkaBroker kafkaProcess;

  private TestContext() {
    kafkaProcess = kafka();
    init();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    close();
                  } catch (Exception e) {
                    LOGGER.error("Error during test shutdown", e);
                  }
                }));
  }

  private void init() {
  }

  private static EmbeddedKafkaBroker kafka() {
    EmbeddedKafkaBroker kafkaEmbedded =
        new EmbeddedKafkaBroker(
            3,
            false,
            1,
            "my-test-topic-upstream", "my-test-topic-downstream");
    Map<String, String> brokerProperties = new HashMap<>();
    brokerProperties.put("default.replication.factor", "1");
    brokerProperties.put("offsets.topic.replication.factor", "1");
    brokerProperties.put("group.initial.rebalance.delay.ms", "3000");
    kafkaEmbedded.brokerProperties(brokerProperties);
    try {
      kafkaEmbedded.afterPropertiesSet();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return kafkaEmbedded;
  }

  @Override
  public void close() throws Exception {
    kafkaProcess.destroy();
  }

  static TestContext build(String key) {
    return new TestContext();
  }
}
