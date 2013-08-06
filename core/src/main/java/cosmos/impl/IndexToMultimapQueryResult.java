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
package cosmos.impl;

import java.nio.charset.CharacterCodingException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.google.common.base.Function;

import cosmos.Cosmos;
import cosmos.UnexpectedStateException;
import cosmos.options.Defaults;
import cosmos.results.impl.MultimapQueryResult;

/**
 * 
 */
public class IndexToMultimapQueryResult implements Function<Entry<Key,Value>,MultimapQueryResult> {
  
  protected final Cosmos sorts;
  protected final SortableResult id;
  
  public IndexToMultimapQueryResult(Cosmos sorts, SortableResult id) {
    this.sorts = sorts;
    this.id = id;
  }
  
  @Override
  public MultimapQueryResult apply(Entry<Key,Value> input) {
    Key k = input.getKey();
    
    Text colqual = k.getColumnQualifier();
    
    int index = colqual.find(Defaults.NULL_BYTE_STR);
    if (-1 == index) {
      throw new RuntimeException("Was provided unexpected Key: " + k);
    }
    
    int start = index + 1;
    try {
      String docId = Text.decode(colqual.getBytes(), start, colqual.getLength() - start);
      
      return sorts.contents(id, docId);
      
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    } catch (UnexpectedStateException e) {
      throw new RuntimeException(e);
    } catch (CharacterCodingException e) {
      throw new RuntimeException(e);
    }
  }
  
}
