package sorts.results;

import org.apache.accumulo.core.security.ColumnVisibility;

import com.google.common.base.Preconditions;

public class Value {
  private final String value;
  private final ColumnVisibility visibility;
  
  public Value(String value, ColumnVisibility visibility) {
    Preconditions.checkNotNull(value);
    Preconditions.checkNotNull(visibility);
    this.value = value;
    this.visibility = visibility;
  }
  
  public String value() {
    return this.value;
  }
  
  public ColumnVisibility visibility() {
    return this.visibility;
  }
  
  public static Value create(String value, ColumnVisibility visibility) {
    Preconditions.checkNotNull(value);
    Preconditions.checkNotNull(visibility);
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
