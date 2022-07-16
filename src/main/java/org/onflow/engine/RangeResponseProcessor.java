package org.onflow.engine;

import java.util.Map;
import java.util.stream.Collectors;

public class RangeResponseProcessor {
  private final long activeRangeSize;
  private final int minRangeResponse;
  private final Map<Long, Integer> heightToProcessedBlocks;

  public RangeResponseProcessor(long activeRangeSize, int minRangeResponse, Map<Long, Integer> heightToProcessedBlocks) {
    this.activeRangeSize = activeRangeSize;
    this.minRangeResponse = minRangeResponse;
    this.heightToProcessedBlocks = heightToProcessedBlocks;
  }

  public void processRange(long startHeight, Block[] blocks) {
    long i = startHeight;
    for (Block block : blocks) {
      heightToProcessedBlocks.merge(i, 1, Integer::sum);
      i++;
    }
  }

  public ActiveRange getActiveRange() {
    long minHeight = heightToProcessedBlocks.entrySet()
        .stream()
        .filter((entry) -> entry.getValue() < this.minRangeResponse)
        .collect(Collectors.toList())
        .get(0)
        .getKey();
    return new ActiveRange(minHeight, minHeight + activeRangeSize - 1);
  }
}
