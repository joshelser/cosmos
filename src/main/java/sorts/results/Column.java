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
  
  @Override
  public int hashCode() {
    return this.column.hashCode();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Column) {
      return this.column.equals(((Column) o).column());
    }
    
    return false;
  }
  
  @Override
  public String toString() {
    return column.toString();
  }
}
