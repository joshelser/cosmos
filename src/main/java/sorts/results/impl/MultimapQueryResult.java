package sorts.results.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

import sorts.results.Column;
import sorts.results.QueryResult;
import sorts.results.SValue;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class MultimapQueryResult implements QueryResult<MultimapQueryResult> {
  
  protected String docId;
  protected Multimap<Column,SValue> document;
  protected ColumnVisibility docVisibility;
  
  protected MultimapQueryResult() { } 
  
  public <T1,T2> MultimapQueryResult(Multimap<T1,T2> untypedDoc, String docId, ColumnVisibility docVisibility,
      Function<Entry<T1,T2>,Entry<Column,SValue>> function) {
    checkNotNull(untypedDoc);
    checkNotNull(docId);
    checkNotNull(docVisibility);
    checkNotNull(function);
    
    this.docId = docId;
    this.docVisibility = docVisibility;
    this.document = HashMultimap.create();
    
    for (Entry<T1,T2> untypedEntry : untypedDoc.entries()) {
      Entry<Column,SValue> entry = function.apply(untypedEntry);
      this.document.put(entry.getKey(), entry.getValue());
    }
  }
  
  public MultimapQueryResult(Multimap<Column,SValue> document, String docId, ColumnVisibility docVisibility) {
    checkNotNull(document);
    checkNotNull(docId);
    checkNotNull(docVisibility);
    
    this.document = document;
    this.docId = docId;
    this.docVisibility = docVisibility;
  }
  
  public MultimapQueryResult(MultimapQueryResult other, String newDocId) {
    checkNotNull(other);
    checkNotNull(newDocId);
    
    this.docId = newDocId;
    this.document = HashMultimap.create(other.document);
    this.docVisibility = other.docVisibility;
  }
  
  public String docId() {
    return this.docId;
  }
  
  public String document() {
    return this.document.toString();
  }
  
  public MultimapQueryResult typedDocument() {
    return this;
  }
  
  public ColumnVisibility documentVisibility() {
    return this.docVisibility;
  }
  
  public Iterable<Entry<Column,SValue>> columnValues() {
    return this.document.entries();
  }
  
  public static MultimapQueryResult recreate(DataInput in) throws IOException {
    MultimapQueryResult result = new MultimapQueryResult();
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
    this.document = HashMultimap.create(); 
    
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
    for (Entry<Column,SValue> entry : this.document.entries()) {
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
  
}
