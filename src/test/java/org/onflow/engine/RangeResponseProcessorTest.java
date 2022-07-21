package org.onflow.engine;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


class RangeResponseProcessorTest {

  @Test
  void rangeProcessorCanBeCreated() {
    long activeRangeSize = 100L;
    int minRangeResponse = 10;
    RangeResponseProcessor r = new RangeResponseProcessor(activeRangeSize, minRangeResponse, new ConcurrentHashMap<>());
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
    ConcurrentMap<Long, Integer> heightToProcessedBlocks = new ConcurrentHashMap<>();
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
    ConcurrentMap<Long, Integer> heightToProcessedBlocks = new ConcurrentHashMap<>();
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

  @Test
  void blocksNotWithinActiveRangeAreIgnored() {
    long activeRangeSize = 3L;
    ConcurrentMap<Long, Integer> heightToProcessedBlocks = new ConcurrentHashMap<>();
    RangeResponseProcessor processor = new RangeResponseProcessor(activeRangeSize, 10, heightToProcessedBlocks);
    Block[] blocks = new Block[]{
        new Block("first"),
        new Block("second"),
        new Block("third"),
        new Block("fourth"),  // has to be ignored with active range size 3
        new Block("fifth"),   // has to be ignored with active range size 3
    };

    processor.processRange(0L, blocks);
    Map<Long, Integer> expectedResult = new HashMap<>() {{
      put(0L, 1);
      put(1L, 1);
      put(2L, 1);
    }};
    assertEquals(expectedResult, heightToProcessedBlocks);

  }

  @Test
  void initialActiveRangeIsZeroToActiveRangeSizeMinusOne() {
    long activeRangeSize = 5L;
    RangeResponseProcessor processor = new RangeResponseProcessor(activeRangeSize, 10, new ConcurrentHashMap<>());
    ActiveRange expected = new ActiveRange(0L, activeRangeSize - 1);
    assertEquals(expected, processor.getActiveRange());
  }

  @Test
  void activeRangeIsReturnedWithMinHeightThatHasLesThanMinResponses() {
    int minRangeResponse = 10;
    long activeRangeSize = 5L;
    ConcurrentMap<Long, Integer> heightToProcessedBlocks = new ConcurrentHashMap<>() {{
      put(0L, 9); // gets updated and fulfilled
      put(1L, 10);
      put(2L, 7); // min height that is < than
      put(5L, 8);
      put(6L, 10);
      put(7L, 10);
      put(10L, 10);
    }};

    RangeResponseProcessor processor = new RangeResponseProcessor(activeRangeSize, minRangeResponse, heightToProcessedBlocks);
    processor.processRange(0, new Block[]{new Block("first")});
    ActiveRange expected = new ActiveRange(2L, 2L + activeRangeSize - 1);
    assertEquals(expected, processor.getActiveRange());
  }

  @Test
  void processRangeCanProcessBlocksInThreadSafeManner() throws ExecutionException, InterruptedException {
    Block[] blocks = new Block[]{
        new Block("first"),
    };
    int threads = 1000;
    ExecutorService service = Executors.newFixedThreadPool(threads);
    CountDownLatch latch = new CountDownLatch(1);
    Collection<Future<?>> futures = new ArrayList<>(threads);
    Map<Long, Integer> heightToProcessedBlocks = new HashMap<>();
    RangeResponseProcessor processor = new RangeResponseProcessor(1L, 20, heightToProcessedBlocks, new UpdateListener(0, 20));

    for (int i = 0; i < threads; i++) {
      futures.add(
          service.submit(() -> {
            try {
              latch.await();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            processor.processRange(0, blocks);
          })
      );
    }
    latch.countDown();
    for (Future<?> f : futures) {
        f.get();
    }
    assertEquals(heightToProcessedBlocks.get(0L), threads);
  }

}