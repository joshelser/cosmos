/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cosmos.records.values;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.WritableUtils;

/**
 * 
 */
public class BooleanRecordValue extends RecordValue<Boolean> {
  private static final StringLexicoder lexer = new StringLexicoder();
  private static final ReverseLexicoder<String> revLexer = new ReverseLexicoder<String>(lexer);
  
  protected BooleanRecordValue() { }
  
  public BooleanRecordValue(Boolean b, ColumnVisibility cv) {
    super(b, cv);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    readVisibility(in);
    value = Boolean.parseBoolean(WritableUtils.readString(in));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, BooleanRecordValue.class.getName());
    writeVisibility(out);
    WritableUtils.writeString(out, value.toString());
  }

  @Override
  public RecordValueType type() {
    return RecordValueType.BOOLEAN;
  }

  @Override
  public byte[] lexicographicValue() {
    return lexer.encode(value.toString());
  }

  @Override
  public byte[] reverseLexicographicValue() {
    return revLexer.encode(value.toString());
  }

}
