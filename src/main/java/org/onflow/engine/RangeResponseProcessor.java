package org.onflow.engine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RangeResponseProcessor {
  private final long activeRangeSize;
  private final int minRangeResponse;
  private final AtomicLong minUnfulfilledHeight;
  private final Map<Long, Integer> heightToProcessedBlocks;
  private UpdateListener updateListener;

  public RangeResponseProcessor(long activeRangeSize, int minRangeResponse) {
    this.activeRangeSize = activeRangeSize;
    this.minRangeResponse = minRangeResponse;
    this.heightToProcessedBlocks = new ConcurrentHashMap<>();
    minUnfulfilledHeight = new AtomicLong(0);
  }

  RangeResponseProcessor(long activeRangeSize,
                         int minRangeResponse,
                         Map<Long, Integer> heightToProcessedBlocks) {
    this.activeRangeSize = activeRangeSize;
    this.minRangeResponse = minRangeResponse;
    this.heightToProcessedBlocks = heightToProcessedBlocks;
    this.minUnfulfilledHeight = new AtomicLong(0);
    updateActiveRange();
  }

  RangeResponseProcessor(long activeRangeSize,
                         int minRangeResponse,
                         Map<Long, Integer> heightToProcessedBlocks,
                         UpdateListener updateListener) {
    this.activeRangeSize = activeRangeSize;
    this.minRangeResponse = minRangeResponse;
    this.heightToProcessedBlocks = heightToProcessedBlocks;
    this.minUnfulfilledHeight = new AtomicLong(0);
    this.updateListener = updateListener;
    updateActiveRange();
  }

  public synchronized void processRange(long startHeight, Block[] blocks) {
    if (blocks != null) {
      ActiveRange activeRange = getActiveRange();
      startHeight = Math.max(startHeight, activeRange.getMinHeight());
      boolean activeRangeUpdateRequired = false;
      for (long i = startHeight; (i < blocks.length + startHeight && i <= activeRange.getMaxHeight()); i++) {
        heightToProcessedBlocks.merge(i, 1, Integer::sum);
        if (isActiveRangeUpdateRequired(i)) {
          activeRangeUpdateRequired = true;
        }
      }
      if (activeRangeUpdateRequired) {
        updateActiveRange();
      }
    }
  }

  public ActiveRange getActiveRange() {
    long h = minUnfulfilledHeight.get();
    return new ActiveRange(h, h + activeRangeSize - 1);
  }

  private boolean isActiveRangeUpdateRequired(long blockHeight) {
    return blockHeight == minUnfulfilledHeight.get() &&
        allResponseReceived(blockHeight) &&
        heightToProcessedBlocks.get(blockHeight) == minRangeResponse;
  }

  private boolean allResponseReceived(long blockHeight) {
    Integer responsesNumber = heightToProcessedBlocks.get(blockHeight);
    if (responsesNumber == null) {
      return false;
    }
    return responsesNumber == minRangeResponse;
  }

  private void updateActiveRange() {
    if (updateListener != null) {
      updateListener.consumeUpdate(heightToProcessedBlocks);
    }
    long i = minUnfulfilledHeight.get();
    while (i < heightToProcessedBlocks.size() &&
        (!heightToProcessedBlocks.containsKey(i) || heightToProcessedBlocks.get(i) >= minRangeResponse)) {
      minUnfulfilledHeight.getAndIncrement();
      i++;
    }
  }
}
