package sorts.results.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import sorts.results.Column;
import sorts.results.QueryResult;
import sorts.results.Value;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

public class MapQueryResult implements QueryResult<MapQueryResult> {
  
  protected final ByteBuffer docId;
  protected final Map<Column,Value> document;
  
  public <T1,T2> MapQueryResult(Map<T1,T2> untypedDoc, ByteBuffer docId, Function<Entry<T1,T2>, Entry<Column,Value>> function) {
    checkNotNull(untypedDoc);
    checkNotNull(docId);
    checkNotNull(function);
    
    this.docId = docId;
    this.document = Maps.newHashMapWithExpectedSize(untypedDoc.size());
    
    for (Entry<T1,T2> untypedEntry : untypedDoc.entrySet()) {
      Entry<Column,Value> entry = function.apply(untypedEntry);
      this.document.put(entry.getKey(), entry.getValue());
    }
  }
  
  public MapQueryResult(Map<Column,Value> document, ByteBuffer docId) {
    checkNotNull(document);
    checkNotNull(docId);
    
    this.document = document;
    this.docId = docId;
  }

  public ByteBuffer docId() {
    return this.docId;
  }

  public ByteBuffer document() {
    return ByteBuffer.wrap(this.document.toString().getBytes());
  }

  public MapQueryResult typedDocument() {
    return this;
  }

  public Iterable<Entry<Column,Value>> columnValues() {
    return this.document.entrySet();
  }
  
}
