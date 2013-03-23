package sorts.results;

import java.nio.ByteBuffer;

public class Column {
  private final ByteBuffer column;
  
  public Column(ByteBuffer column) {
    this.column = column;
  }

  public ByteBuffer column() {
    return this.column;
  }
  
  public static Column create(ByteBuffer column) {
    return new Column(column);
  }
}
