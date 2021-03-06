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
package cosmos.sql;

import java.util.Iterator;

import com.google.common.collect.Iterators;

/**
 * Defines an accumulo iterable based on iterable
 * 
 * @param <T>
 */
public class BaseIterable<T> implements Iterable<T> {

  Iterator<T> kvIter;

  public BaseIterable() {
    kvIter = Iterators.emptyIterator();
  }

  public BaseIterable(Iterator<T> uter) {
    kvIter = uter;

  }

  @Override
  public Iterator<T> iterator() {

    return kvIter;
  }

}
