package sorts.results.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

import sorts.results.Column;
import sorts.results.QueryResult;
import sorts.results.SValue;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

public class MapQueryResult implements QueryResult<MapQueryResult> {
  
  protected String docId;
  protected Map<Column,SValue> document;
  protected ColumnVisibility docVisibility;
  
  protected MapQueryResult() { }
  
  public <T1,T2> MapQueryResult(Map<T1,T2> untypedDoc, String docId, ColumnVisibility docVisibility, Function<Entry<T1,T2>,Entry<Column,SValue>> function) {
    checkNotNull(untypedDoc);
    checkNotNull(docId);
    checkNotNull(docVisibility);
    checkNotNull(function);
    
    this.docId = docId;
    this.document = Maps.newHashMapWithExpectedSize(untypedDoc.size());
    this.docVisibility = docVisibility;
    
    for (Entry<T1,T2> untypedEntry : untypedDoc.entrySet()) {
      Entry<Column,SValue> entry = function.apply(untypedEntry);
      this.document.put(entry.getKey(), entry.getValue());
    }
  }
  
  public MapQueryResult(Map<Column,SValue> document, String docId, ColumnVisibility docVisibility) {
    checkNotNull(document);
    checkNotNull(docId);
    checkNotNull(docVisibility);
    
    this.document = document;
    this.docId = docId;
    this.docVisibility = docVisibility;
  }
  
  public String docId() {
    return this.docId;
  }
  
  public String document() {
    return this.document.toString();
  }
  
  public MapQueryResult typedDocument() {
    return this;
  }
  
  public ColumnVisibility documentVisibility() {
    return this.docVisibility;
  }
  
  public Iterable<Entry<Column,SValue>> columnValues() {
    return this.document.entrySet();
  }
  
  public static MapQueryResult recreate(DataInput in) throws IOException {
    MapQueryResult result = new MapQueryResult();
    result.readFields(in);
    return result;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.docId = Text.readString(in);
    
    final int cvLength = WritableUtils.readVInt(in);
    final byte[] cvBytes = new byte[cvLength];
    in.readFully(cvBytes);
    
    this.docVisibility = new ColumnVisibility(cvBytes);
    
    final int entryCount = WritableUtils.readVInt(in);
    this.document = Maps.newHashMapWithExpectedSize(entryCount);
    
    for (int i = 0; i < entryCount; i++) {
      
      this.document.put(Column.recreate(in), SValue.recreate(in));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.docId);

    byte[] cvBytes = this.docVisibility.flatten();
    WritableUtils.writeVInt(out, cvBytes.length);
    out.write(cvBytes);
    
    WritableUtils.writeVInt(out, this.document.size());
    for (Entry<Column,SValue> entry : this.document.entrySet()) {
      entry.getKey().write(out);
      entry.getValue().write(out);
    }
  }
  
  @Override
  public Value toValue() throws IOException {
    DataOutputBuffer buf = new DataOutputBuffer();
    this.write(buf);
    buf.close();
    byte[] bytes = new byte[buf.getLength()];
    System.arraycopy(buf.getData(), 0, bytes, 0, buf.getLength());
    
    return new Value(bytes);
  }
  
  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder(17,31);
    hcb.append(this.docId);
    hcb.append(this.docVisibility.hashCode());
    hcb.append(this.document.hashCode());
    return hcb.toHashCode();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof MultimapQueryResult) {
      MultimapQueryResult other = (MultimapQueryResult) o;
      return this.docId.equals(other.docId) && this.docVisibility.equals(other.docVisibility) &&
          this.document.equals(other.document);
    }
    
    return false;
  }
  
}
