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
package cosmos.records;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import cosmos.options.Defaults;
import cosmos.records.values.DoubleRecordValue;
import cosmos.records.values.IntegerRecordValue;
import cosmos.records.values.LongRecordValue;
import cosmos.records.values.RecordValue;
import cosmos.records.values.StringRecordValue;

/**
 * 
 */
public class RecordValueTypeAdapter extends TypeAdapter<RecordValue<?>> {

  @Override
  public void write(JsonWriter out, RecordValue<?> recordValue) throws IOException {
    if (null == recordValue) {
      out.nullValue();
      return;
    }
    
    // Unravel the concrete RecordValue type to ensure we write out the correct JSON value
    switch (recordValue.type()) {
      case NUMBER: {
        if (recordValue instanceof IntegerRecordValue) {
          out.value((Integer) recordValue.value());
        } else if (recordValue instanceof LongRecordValue) {
          out.value((Long) recordValue.value());
        } else if (recordValue instanceof DoubleRecordValue) {
          out.value((Double) recordValue.value());
        } else {
          throw new RuntimeException("Can't handle NumberRecordValue " + recordValue + " of class " + recordValue.getClass());
        }
        break;
      }
      case STRING: {
        out.value((String)recordValue.value());
        break;
      }
      default: {
        throw new RuntimeException("Can't handle RecordValue " + recordValue + " of class " + recordValue.getClass());
      }
    }
  }

  @Override
  public RecordValue<?> read(JsonReader in) throws IOException {
    final JsonToken tok = in.peek();
    if (tok == JsonToken.NULL) {
      in.nextNull();
      return null;
    }
    
    if (tok == JsonToken.STRING) {
      return new StringRecordValue(in.nextString(), Defaults.EMPTY_VIS);
    } else if (tok == JsonToken.NUMBER){
      double d = in.nextDouble();
      if ((int) d == d) {
        return new IntegerRecordValue((int) d, Defaults.EMPTY_VIS);
      } else if ((long) d == d) {
        return new LongRecordValue((long) d, Defaults.EMPTY_VIS);
      } else {
        return new DoubleRecordValue(d, Defaults.EMPTY_VIS);
      }
    }
    
    throw new RuntimeException("Expected String or Number. Can't process JSON token: " + tok);
  }

}
