package org.onflow.engine;

import java.util.Map;

public class RangeResponseProcessor {
  private final Map<Long, Integer> heightToProcessedBlocks;

  public RangeResponseProcessor(Map<Long, Integer> heightToProcessedBlocks) {
    this.heightToProcessedBlocks = heightToProcessedBlocks;
  }

  public void processRange(long startHeight, Block[] blocks) {
    long i = startHeight;
    for (Block block : blocks) {
      heightToProcessedBlocks.merge(i, 1, Integer::sum);
      i++;
    }
  }
}
