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
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

import cosmos.options.Defaults;

/**
 * This is apt to be stupidly slow. Perhaps use a bloomfilter instead?
 * Perhaps the API itself should just allow the client to specify when it
 * cares about getting dupes.
 */
public class DedupingPredicate implements Predicate<Entry<Key,Value>> {

  protected final Set<String> uids;
  private final Text holder;
  
  public DedupingPredicate() {
    uids = Sets.newHashSetWithExpectedSize(64);
    holder = new Text();
  }
  
  @Override
  public boolean apply(Entry<Key,Value> input) {
    Preconditions.checkNotNull(input);
    Preconditions.checkNotNull(input.getKey());
    
    input.getKey().getColumnQualifier(holder);
    
    int index = holder.find(Defaults.NULL_BYTE_STR);
    
    Preconditions.checkArgument(-1 != index);
    
    String uid = null;
    try {
      uid = Text.decode(holder.getBytes(), index + 1, holder.getLength() - (index + 1));
    } catch (CharacterCodingException e) {
      throw new RuntimeException(e);
    }
    
    // If we haven't seen this UID yet, note such, and then keep this item
    if (!uids.contains(uid)) {
      uids.add(uid);
      return true;
    }
    
    // Otherwise, don't re-return this item
    return false;
  }
  
}
