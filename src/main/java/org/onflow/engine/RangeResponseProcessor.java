package org.onflow.engine;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class RangeResponseProcessor {
  private final long activeRangeSize;
  private final int minRangeResponse;
  private final ConcurrentMap<Long, Integer> heightToProcessedBlocks;

  public RangeResponseProcessor(long activeRangeSize, int minRangeResponse, ConcurrentMap<Long, Integer> heightToProcessedBlocks) {
    this.activeRangeSize = activeRangeSize;
    this.minRangeResponse = minRangeResponse;
    this.heightToProcessedBlocks = heightToProcessedBlocks;
  }

  public void processRange(long startHeight, Block[] blocks) {
    long i = startHeight;
    ActiveRange activeRange = getActiveRange();
    for (Block block : blocks) {
      heightToProcessedBlocks.merge(i, 1, Integer::sum);
      i++;
      if (i > activeRange.getMaxHeight()) {
        break;
      }
    }
  }

  public ActiveRange getActiveRange() {
    List<Map.Entry<Long, Integer>> unfulfilledHeights = heightToProcessedBlocks.entrySet()
        .stream()
        .filter((entry) -> entry.getValue() < this.minRangeResponse)
        .sorted(Comparator.comparingLong(Map.Entry::getKey))
        .collect(Collectors.toList());

    if (unfulfilledHeights.isEmpty()) {
      return new ActiveRange(0L, activeRangeSize - 1);
    }
    long minHeight = unfulfilledHeights.get(0).getKey();

    return new ActiveRange(minHeight, minHeight + activeRangeSize - 1);
  }
}
