package org.onflow.engine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class RangeResponseProcessor {
  private final long activeRangeSize;
  private final int minRangeResponse;
  private final AtomicLong minUnfulfilledHeight;
  private final ConcurrentMap<Long, Integer> heightToProcessedBlocks;

  public RangeResponseProcessor(long activeRangeSize, int minRangeResponse) {
    this.activeRangeSize = activeRangeSize;
    this.minRangeResponse = minRangeResponse;
    this.heightToProcessedBlocks = new ConcurrentHashMap<>();
    minUnfulfilledHeight = new AtomicLong(0);
  }

  RangeResponseProcessor(long activeRangeSize, int minRangeResponse, ConcurrentMap<Long, Integer> heightToProcessedBlocks) {
    this.activeRangeSize = activeRangeSize;
    this.minRangeResponse = minRangeResponse;
    this.heightToProcessedBlocks = heightToProcessedBlocks;
    minUnfulfilledHeight = new AtomicLong(0);
  }

  public void processRange(long startHeight, Block[] blocks) {
    for (long i = startHeight; i < blocks.length + startHeight; i++) {
      heightToProcessedBlocks.merge(i, 1, Integer::sum);
      if (isActiveRangeUpdateRequired(i)) {
        updateActiveRange();
      }
      if (i + 1 > getActiveRange().getMaxHeight()) {
        break;
      }
    }
  }

  public ActiveRange getActiveRange() {
    long h = minUnfulfilledHeight.get();
    return new ActiveRange(h, h + activeRangeSize - 1);
  }

  private boolean isActiveRangeUpdateRequired(long blockHeight) {
    return allResponseReceived(blockHeight) && heightToProcessedBlocks.get(blockHeight) == minRangeResponse;
  }

  private boolean allResponseReceived(long blockHeight) {
    Integer responsesNumber = heightToProcessedBlocks.get(blockHeight);
    if (responsesNumber == null) {
      return false;
    }
    return responsesNumber == minRangeResponse;
  }

  private void updateActiveRange() {
    long i = minUnfulfilledHeight.incrementAndGet();
    while (heightToProcessedBlocks.get(i) >= minRangeResponse && i < heightToProcessedBlocks.size()) {
      minUnfulfilledHeight.getAndIncrement();
      i++;
    }
  }
}
