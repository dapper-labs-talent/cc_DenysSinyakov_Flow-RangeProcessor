package org.onflow.engine;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


class RangeResponseProcessorTest {

  @Test
  void rangeProcessorCanBeCreated() {
    long activeRangeSize = 100L;
    int minRangeResponse = 10;
    RangeResponseProcessor r = new RangeResponseProcessor(activeRangeSize, minRangeResponse, new HashMap<>());
    assertNotNull(r);
  }

  @Test
  void processRangeCanProcessSingleBlock() {
    Block[] blocks = new Block[]{
        new Block("first"),
        new Block("second"),
        new Block("third")
    };
    long startHeight = 0;
    Map<Long, Integer> heightToProcessedBlocks = new HashMap<>();
    RangeResponseProcessor processor = new RangeResponseProcessor(100L, 10, heightToProcessedBlocks);
    processor.processRange(startHeight, blocks);
    Map<Long, Integer> expectedResult = new HashMap<>() {{
      put(0L, 1);
      put(1L, 1);
      put(2L, 1);
    }};
    assertEquals(expectedResult, heightToProcessedBlocks);
  }

  @Test
  void processRangeCanProcessConsecutiveRangeOfBlocks() {
    Block[] blocks = new Block[]{
        new Block("first"),
        new Block("second"),
        new Block("third")
    };
    Map<Long, Integer> heightToProcessedBlocks = new HashMap<>();
    RangeResponseProcessor processor = new RangeResponseProcessor(100L, 10, heightToProcessedBlocks);
    processor.processRange(0, blocks);
    processor.processRange(1, blocks);
    processor.processRange(1, blocks);
    Map<Long, Integer> expectedResult = new HashMap<>() {{
      put(0L, 1);
      put(1L, 3);
      put(2L, 3);
      put(3L, 2);
    }};
    assertEquals(expectedResult, heightToProcessedBlocks);
  }

}