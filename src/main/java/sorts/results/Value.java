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
  
  @Override
  public int hashCode() {
    return this.value.hashCode() ^ this.visibility.hashCode();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Value) {
      Value other = (Value) o;
      return this.value.equals(other.value) &&
          this.visibility.equals(other.visibility);
    }
    
    return false;
  }
  
  @Override
  public String toString() {
    return visibility + ": " + value;
  }
}
