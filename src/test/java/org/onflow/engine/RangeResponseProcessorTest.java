package org.onflow.engine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;


class RangeResponseProcessorTest {

  @Test
  void rangeProcessorCanBeCreated() {
    RangeResponseProcessor r = new RangeResponseProcessor();
    assertNotNull(r);
  }

}