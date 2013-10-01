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
 *  Copyright 2013 
 *
 */
package cosmos.sql.call;

import java.util.Collection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * Base visitor of type T, contains all children in a multimap
 * 
 * @param <T>
 */
public class BaseVisitor<T extends CallIfc<?>> implements CallIfc<T> {

  protected Multimap<String,T> children;

  public BaseVisitor() {
    children = ArrayListMultimap.create();
  }

  @Override
  public CallIfc<?> addChild(String id, T child) {
    children.put(id, child);
    return this;
  }

  public Collection<T> children(String id) {
    return children.get(id);
  }

  public Collection<String> childrenIds() {
    return children.keySet();
  }

}
