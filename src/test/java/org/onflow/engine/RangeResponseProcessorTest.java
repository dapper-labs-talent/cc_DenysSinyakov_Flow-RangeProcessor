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

  private static final long ACTIVE_RANGE_SIZE = 100L;
  private static final int MIN_RANGE_RESPONSE = 10;

  @Test
  void rangeProcessorCanBeCreated() {
    RangeResponseProcessor responseProcessor =
        new RangeResponseProcessor(ACTIVE_RANGE_SIZE, MIN_RANGE_RESPONSE, new ConcurrentHashMap<>());
    assertNotNull(responseProcessor);
  }

  @Test
  void processRangeCanProcessSingleBlock() {
    Block[] blocks = generateBlockArrayOfSize(3);
    long startHeight = 0;
    ConcurrentMap<Long, Integer> heightToProcessedBlocks = new ConcurrentHashMap<>();
    RangeResponseProcessor processor =
        new RangeResponseProcessor(ACTIVE_RANGE_SIZE, MIN_RANGE_RESPONSE, heightToProcessedBlocks);
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
    Block[] blocks = generateBlockArrayOfSize(3);
    ConcurrentMap<Long, Integer> heightToProcessedBlocks = new ConcurrentHashMap<>();
    RangeResponseProcessor processor =
        new RangeResponseProcessor(ACTIVE_RANGE_SIZE, MIN_RANGE_RESPONSE, heightToProcessedBlocks);
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
  void blocksNotWithinInitialActiveRangeAreIgnored() {
    ConcurrentMap<Long, Integer> heightToProcessedBlocks = new ConcurrentHashMap<>();
    long activeRangeSize = 3L;
    RangeResponseProcessor processor =
        new RangeResponseProcessor(activeRangeSize, MIN_RANGE_RESPONSE, heightToProcessedBlocks);
    Block[] blocks = generateBlockArrayOfSize(5); // blocks of index 3 and 4 to be ignored with active range size 3

    processor.processRange(0L, blocks);
    Map<Long, Integer> expectedResult = new HashMap<>() {{
      put(0L, 1);
      put(1L, 1);
      put(2L, 1);
    }};
    assertEquals(expectedResult, heightToProcessedBlocks);
  }

  @Test
  void blocksNotWithinActiveRangeAreIgnored() {
    ConcurrentMap<Long, Integer> heightToProcessedBlocks = new ConcurrentHashMap<>() {{
      put(0L, 10);
      put(1L, 10);
      put(2L, 1);
      put(3L, 1);
    }};
    long activeRangeSize = 3L;
    RangeResponseProcessor processor =
        new RangeResponseProcessor(activeRangeSize, MIN_RANGE_RESPONSE, heightToProcessedBlocks);
    Block[] blocks = generateBlockArrayOfSize(10);

    processor.processRange(0L, blocks);
    Map<Long, Integer> expectedResult = new HashMap<>() {{
      put(0L, 10); // should remain the same
      put(1L, 10); // should remain the same
      put(2L, 2);  // received 2 responses
      put(3L, 2);  // received 1 response
      put(4L, 1);  // received 1 response
      // the rest should be ignored since the active range size is 3
    }};
    assertEquals(expectedResult, heightToProcessedBlocks);
  }

  @Test
  void initialActiveRangeIsZeroToActiveRangeSizeMinusOne() {
    RangeResponseProcessor processor =
        new RangeResponseProcessor(ACTIVE_RANGE_SIZE, MIN_RANGE_RESPONSE, new ConcurrentHashMap<>());
    ActiveRange expected = new ActiveRange(0L, ACTIVE_RANGE_SIZE - 1);
    assertEquals(expected, processor.getActiveRange());
  }

  @Test
  void activeRangeIsReturnedWithMinHeightThatHasLessThanMinResponses() {
    long activeRangeSize = 5L;
    ConcurrentMap<Long, Integer> heightToProcessedBlocks = new ConcurrentHashMap<>() {{
      put(0L, 9); // gets updated and fulfilled
      put(1L, 10);
      put(2L, 7); // min height that is < than min responses for range
      put(5L, 8);
      put(6L, 10);
      put(7L, 10);
      put(10L, 10);
    }};

    RangeResponseProcessor processor =
        new RangeResponseProcessor(activeRangeSize, MIN_RANGE_RESPONSE, heightToProcessedBlocks);
    processor.processRange(0, new Block[]{new Block("first")});
    ActiveRange expected = new ActiveRange(2L, 2L + activeRangeSize - 1);
    assertEquals(expected, processor.getActiveRange());
  }

  @Test
  void activeRangeIsReturnedWithMinHeightThatHasNoMinResponses() {
    long activeRangeSize = 5L;
    ConcurrentMap<Long, Integer> heightToProcessedBlocks = new ConcurrentHashMap<>() {{
      put(0L, 9); // gets updated and fulfilled
      put(1L, 10);
      put(5L, 0);
    }};

    RangeResponseProcessor processor =
        new RangeResponseProcessor(activeRangeSize, MIN_RANGE_RESPONSE, heightToProcessedBlocks);
    processor.processRange(0, new Block[]{new Block("first")});
    ActiveRange expected = new ActiveRange(2L, 2L + activeRangeSize - 1);
    assertEquals(expected, processor.getActiveRange());
  }

  @Test
  void processRangeCanProcessBlocksInThreadSafeManner() throws ExecutionException, InterruptedException {
    Block[] blocks = generateBlockArrayOfSize(1);
    int threads = 1000;
    int minRangeResponse = 100;
    ExecutorService service = Executors.newFixedThreadPool(threads);
    CountDownLatch latch = new CountDownLatch(1);
    Collection<Future<?>> futures = new ArrayList<>(threads);
    Map<Long, Integer> heightToProcessedBlocks = new HashMap<>();
    RangeResponseProcessor processor = new RangeResponseProcessor(1L, minRangeResponse, heightToProcessedBlocks);

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
    assertEquals(minRangeResponse, heightToProcessedBlocks.get(0L));
  }

  private Block[] generateBlockArrayOfSize(int size) {
    Block[] blocks = new Block[size];
    for (int i = 0; i < size; i++) {
      blocks[i] = new Block(String.valueOf(i));
    }
    return blocks;
  }
}