package org.onflow.engine;

public class Block {
  private String contents;

  public Block(String contents) {
    this.contents = contents;
  }

  public String getContents() {
    return contents;
  }

  public void setContents(String contents) {
    this.contents = contents;
  }
}
