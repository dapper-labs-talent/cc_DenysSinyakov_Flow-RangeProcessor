package org.onflow.engine;

import java.util.Objects;

/**
 * Represents active range values: min height and max height.
 */
public class ActiveRange {
  private final long minHeight;
  private final long maxHeight;

  public ActiveRange(long minHeight, long maxHeight) {
    this.minHeight = minHeight;
    this.maxHeight = maxHeight;
  }

  public long getMinHeight() {
    return minHeight;
  }

  public long getMaxHeight() {
    return maxHeight;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ActiveRange that = (ActiveRange) o;
    return minHeight == that.minHeight && maxHeight == that.maxHeight;
  }

  @Override
  public int hashCode() {
    return Objects.hash(minHeight, maxHeight);
  }

  @Override
  public String toString() {
    return "ActiveRange{" +
        "minHeight=" + minHeight +
        ", maxHeight=" + maxHeight +
        '}';
  }
}
