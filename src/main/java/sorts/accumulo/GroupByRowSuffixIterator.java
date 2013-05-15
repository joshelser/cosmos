package sorts.accumulo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;

/**
 * 
 */
public class GroupByRowSuffixIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

  protected SortedKeyValueIterator<Key,Value> source;
  protected Key topKey = null;
  protected VLongWritable count = null;
  
  private final Text _holder = new Text(); 
  
  public GroupByRowSuffixIterator() {
    this.count = new VLongWritable();
  }
  
  public GroupByRowSuffixIterator(GroupByRowSuffixIterator other, IteratorEnvironment env) {
    this();
    this.source = other.getSource().deepCopy(env);
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    setSource(source);
    
    // Ensure we were given valid options
    if (!validateOptions(options)) {
      throw new IllegalStateException("Could not initialize " + this.getClass().getName() + " with options: " + options);
    }
    
    setOptions(options);
  }

  @Override
  public boolean hasTop() {
    return null != topKey;
  }

  @Override
  public void next() throws IOException {
    // After the last call to countKeys(), our source's topKey is already
    // incremented to the key past the "row" we just counted
    countKeys();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    getSource().seek(range, columnFamilies, inclusive);
    
    countKeys();
  }

  @Override
  public Key getTopKey() {
    return this.topKey;
  }

  @Override
  public Value getTopValue() {
    return getValue(this.count);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new GroupByRowSuffixIterator(this, env);
  }

  protected SortedKeyValueIterator<Key,Value> getSource() {
    return this.source;
  }
  
  protected void setSource(SortedKeyValueIterator<Key,Value> source) {
    this.source = source;
  }
  
  @Override
  public IteratorOptions describeOptions() {
    return null;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    return true;
  }
  
  protected void setOptions(Map<String,String> options) {
    
  }

  protected void countKeys() throws IOException {
    // No data, nothing to do
    if (!getSource().hasTop()) {
      this.topKey = null;
      this.count.set(0);
      return;
    }
    
    this.topKey = getSource().getTopKey();
    this.topKey.getRow(_holder);
    
    final Range searchSpace = Range.exact(_holder);
    long keyCount = 0;
    Key currentKey = this.topKey;
    
    // While we're still within the desired search space (this row) 
    while (searchSpace.contains(currentKey)) {
      keyCount++;
      getSource().next();
     
      if (!getSource().hasTop()) {
        break;
      }
      
      currentKey = getSource().getTopKey();
    }
    
    this.count.set(keyCount);
  }
  
  
  public static Value getValue(final VLongWritable w) {
    if (w == null) {
      throw new IllegalArgumentException("Writable cannot be null");
    }
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteStream);
    
    // We could also close it, but we know that VLongWritable and BAOS don't need it.
    try {
      w.write(out);
    } catch (IOException e) {
      // If this ever happens, some seriously screwed up is happening or someone subclasses VLongWritable
      // and made it do crazy stuff.
      throw new RuntimeException(e);
    }
    
    return new Value(byteStream.toByteArray());
  }
  
  public static VLongWritable getWritable(final Value v) {
    if (null == v) {
      throw new IllegalArgumentException("Value cannot be null");
    }
    
    ByteArrayInputStream bais = new ByteArrayInputStream(v.get());
    DataInputStream in = new DataInputStream(bais);
    
    VLongWritable writable = new VLongWritable();
    try {
      writable.readFields(in); 
    } catch (IOException e) {
      // If this ever happens, some seriously screwed up is happening or someone subclasses Value
      // and made it do crazy stuff.
      throw new RuntimeException(e);
    }
    
    return writable;
  }
}
