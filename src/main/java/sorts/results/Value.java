package sorts.results;

import java.nio.ByteBuffer;

import org.apache.accumulo.core.security.ColumnVisibility;

public class Value {
  private final ByteBuffer value;
  private final ColumnVisibility visibility;
  
  public Value(ByteBuffer value, ColumnVisibility visibility) {
    this.value = value;
    this.visibility = visibility;
  }
  
  public ByteBuffer value() {
    return this.value;
  }
  
  public ColumnVisibility visibility() {
    return this.visibility;
  }
  
  public static Value create(ByteBuffer value, ColumnVisibility visibility) {
    return new Value(value, visibility);
  }
}
