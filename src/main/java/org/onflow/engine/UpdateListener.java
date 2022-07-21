package org.onflow.engine;

import java.util.Map;

public class UpdateListener {

  private final long blockHeight;
  private final int updatesNumber;

  public UpdateListener(long blockHeight, int updatesNumber) {
    this.blockHeight = blockHeight;
    this.updatesNumber = updatesNumber;
  }

  void consumeUpdate(Map<Long, Integer> blockStats) {
    if (blockStats.get(blockHeight) != updatesNumber) {
      throw new RuntimeException("Number of updates " + blockStats.get(blockHeight) + " is not equal expected " + updatesNumber);
    }
  }
}
