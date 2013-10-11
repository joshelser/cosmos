/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 *  Copyright 2013 Josh Elser
 *
 */
package cosmos.records.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import cosmos.records.Record;
import cosmos.records.RecordFunction;
import cosmos.records.values.RecordValue;
import cosmos.results.Column;

public class MultimapRecord implements Record<MultimapRecord> {

  protected String docId;
  protected Multimap<Column,RecordValue<?>> document;
  protected ColumnVisibility docVisibility;

  protected MultimapRecord() {}

  public <T1,T2> MultimapRecord(Multimap<T1,T2> untypedDoc, String docId, ColumnVisibility docVisibility, RecordFunction<T1,T2> function) {
    checkNotNull(untypedDoc);
    checkNotNull(docId);
    checkNotNull(docVisibility);
    checkNotNull(function);

    this.docId = docId;
    this.docVisibility = docVisibility;
    this.document = HashMultimap.create();

    for (Entry<T1,T2> untypedEntry : untypedDoc.entries()) {
      Entry<Column,RecordValue<?>> entry = function.apply(untypedEntry);
      this.document.put(entry.getKey(), entry.getValue());
    }
  }

  public MultimapRecord(Multimap<Column,RecordValue<?>> document, String docId, ColumnVisibility docVisibility) {
    checkNotNull(document);
    checkNotNull(docId);
    checkNotNull(docVisibility);

    this.document = document;
    this.docId = docId;
    this.docVisibility = docVisibility;
  }

  public MultimapRecord(MultimapRecord other, String newDocId) {
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

  public MultimapRecord typedDocument() {
    return this;
  }

  public ColumnVisibility documentVisibility() {
    return this.docVisibility;
  }

  public Collection<Entry<Column,RecordValue<?>>> columnValues() {
    return this.document.entries();
  }

  public int columnSize() {
    return this.document.keySet().size();
  }

  public boolean containsKey(Column key) {
    return this.document.containsKey(key);
  }

  public boolean containEntry(Column column, RecordValue<?> value) {
    return this.document.containsEntry(column, value);
  }

  public Collection<RecordValue<?>> get(Column column) {
    return this.document.get(column);
  }

  public static MultimapRecord recreate(DataInput in) throws IOException {
    MultimapRecord result = new MultimapRecord();
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

      this.document.put(Column.recreate(in), RecordValue.recreate(in));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.docId);

    byte[] cvBytes = this.docVisibility.getExpression();
    WritableUtils.writeVInt(out, cvBytes.length);
    out.write(cvBytes);

    WritableUtils.writeVInt(out, this.document.size());
    for (Entry<Column,RecordValue<?>> entry : this.document.entries()) {
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
  public String toString() {
    StringBuilder sb = new StringBuilder(256);

    sb.append(this.docId).append(" ").append(this.docVisibility).append(" - ").append(this.document);

    return sb.toString();
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder(17, 31);
    hcb.append(this.docId);
    hcb.append(this.docVisibility.hashCode());
    hcb.append(this.document.hashCode());
    return hcb.toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof MultimapRecord) {
      MultimapRecord other = (MultimapRecord) o;
      return this.docId.equals(other.docId) && this.docVisibility.equals(other.docVisibility) && this.document.equals(other.document);
    }

    return false;
  }

}
