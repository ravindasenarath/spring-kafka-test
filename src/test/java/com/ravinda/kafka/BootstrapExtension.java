package com.ravinda.kafka;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

public class BootstrapExtension implements BeforeAllCallback{
  private static final String CONTEXT = TestContext.class.getName();

  @Override
  public void beforeAll(ExtensionContext context) {
    // Start dependencies and save in the root store

    ExtensionContext.Store store = context.getRoot().getStore(GLOBAL);
    store.getOrComputeIfAbsent(CONTEXT, TestContext::build, TestContext.class);
  }
}
