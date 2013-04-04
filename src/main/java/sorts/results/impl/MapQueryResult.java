package sorts.results.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.security.ColumnVisibility;

import sorts.results.Column;
import sorts.results.QueryResult;
import sorts.results.SValue;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

public class MapQueryResult implements QueryResult<MapQueryResult> {
  
  protected final String docId;
  protected final Map<Column,SValue> document;
  protected final ColumnVisibility docVisibility;
  
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
  
}
