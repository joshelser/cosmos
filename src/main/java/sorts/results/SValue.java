package sorts.results;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;

public class SValue implements Writable, Comparable<SValue> {
  private String value;
  private ColumnVisibility visibility;
  
  protected SValue() { }
  
  public SValue(String value, ColumnVisibility visibility) {
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
  
  public static SValue create(String value, ColumnVisibility visibility) {
    Preconditions.checkNotNull(value);
    Preconditions.checkNotNull(visibility);
    return new SValue(value, visibility);
  }
  
  public static SValue recreate(DataInput in) throws IOException {
    final SValue val = new SValue();
    val.readFields(in);
    return val;
  }
  
  @Override
  public int hashCode() {
    return this.value.hashCode() ^ this.visibility.hashCode();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof SValue) {
      SValue other = (SValue) o;
      return this.value.equals(other.value) &&
          this.visibility.equals(other.visibility);
    }
    
    return false;
  }
  
  @Override
  public String toString() {
    return visibility + ": " + value;
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.value);
    
    byte[] cvBytes = this.visibility.getExpression();
    WritableUtils.writeVInt(out, cvBytes.length);
    out.write(cvBytes);
    
  }

  public void readFields(DataInput in) throws IOException {
    this.value = Text.readString(in);
    
    final int cvLength = WritableUtils.readVInt(in);
    final byte[] cvBytes = new byte[cvLength];
    in.readFully(cvBytes);
    
    this.visibility = new ColumnVisibility(cvBytes);
  }

  @Override
  public int compareTo(SValue o) {
    int res = this.value.compareTo(o.value);
    
    if (0 == res) {
      return this.visibility.toString().compareTo(o.visibility.toString());
    }
    
    return res;
  }
}
