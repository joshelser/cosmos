package sorts.results;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

public class Column {
  private final ByteBuffer column;
  
  public Column(ByteBuffer column) {
    this.column = column;
  }

  public ByteBuffer column() {
    return this.column;
  }
  
  public static Column create(ByteBuffer column) {
    Preconditions.checkNotNull(column);
    
    return new Column(column);
  }
  
  public static Column create(String column) {
    Preconditions.checkNotNull(column);
    return create(ByteBuffer.wrap(column.getBytes()));
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
