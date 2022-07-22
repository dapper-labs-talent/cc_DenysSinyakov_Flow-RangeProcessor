package org.onflow.engine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Processes the blocks sequentially and calculates active range.
 */
public class RangeResponseProcessor {
  private final long activeRangeSize;
  private final int minRangeResponse;
  private final AtomicLong minUnfulfilledHeight;
  private final Map<Long, Integer> heightToProcessedBlocks;

  public RangeResponseProcessor(long activeRangeSize, int minRangeResponse) {
    this.activeRangeSize = activeRangeSize;
    this.minRangeResponse = minRangeResponse;
    this.heightToProcessedBlocks = new ConcurrentHashMap<>();
    minUnfulfilledHeight = new AtomicLong(0);
  }

  /**
   * Processes blocks sequentially. This method also updates the current active range via helper methods.
   *
   * The update process is like following. We start from min unfulfilled block height (h) of 0.
   * Then, if it gets fulfilled (gets all responses), we increment the h until we encounter the next unfulfilled block
   * height. This way we update the update h as we process the range and keep the time O(n) complexity, where n is
   * blocks array size.
   *
   * We made this method synchronized to guarantee thread safety, otherwise we could not guarantee accurate updates of
   * active range as well as logic responsible for ignoring blocks outside the active range since active range could be
   * updated by any thread at anytime producing unpredictable results.
   *
   * @param startHeight start height of the blocks array
   * @param blocks blocks array
   */
  public synchronized void processRange(long startHeight, Block[] blocks) {
    if (blocks != null && startHeight >= 0) {
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
        updateActiveRange(Math.min(activeRange.getMaxHeight(), startHeight + blocks.length));
      }
    }
  }

  /**
   * Calculates current active range.
   * The min height is the current value of {@link RangeResponseProcessor#minUnfulfilledHeight}.
   * The max height is found as {@link RangeResponseProcessor#minUnfulfilledHeight} +
   * {@link RangeResponseProcessor#activeRangeSize} - 1.
   * While we do the calculation there is no guarantee that {@link RangeResponseProcessor#minUnfulfilledHeight} does
   * not change its value, so we store the value first in local var, to make the call thread safe.
   *
   * @return {@link ActiveRange} object containing current active range min and max heights
   */
  public ActiveRange getActiveRange() {
    long h = minUnfulfilledHeight.get();
    return new ActiveRange(h, h + activeRangeSize - 1);
  }

  /**
   * Allows access to block height to responses number map primarily for testing purposes.
   * The visibility is package private for that reason.
   *
   * @return current block stats (block height to responses number map)
   */
  Map<Long, Integer> getBlockStats() {
    return heightToProcessedBlocks;
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

  private void updateActiveRange(long maxHeight) {
    long i = minUnfulfilledHeight.get();
    while (i <= maxHeight && heightToProcessedBlocks.containsKey(i) && heightToProcessedBlocks.get(i) >= minRangeResponse) {
      i++;
    }
    minUnfulfilledHeight.set(i);
  }
}
