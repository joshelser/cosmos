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
package cosmos.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 
 */
public class Single<T> implements Iterable<T> {
  protected final T single;
  
  public Single(T single) {
    checkNotNull(single);
    this.single = single;
  }
  
  public static <T> Single<T> create(T single) {
    return new Single<T>(single);
  }
  
  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      boolean hasNext = true;
      
      @Override
      public boolean hasNext() {
        return hasNext;
      }

      @Override
      public T next() {
        hasNext = false;
        return single;
      }

      @Override
      public void remove() {
        if (!hasNext) {
          throw new NoSuchElementException();
        }
        
        hasNext = false;
      }
      
    };
  }
  
}
