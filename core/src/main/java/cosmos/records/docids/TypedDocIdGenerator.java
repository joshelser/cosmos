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
package cosmos.records.docids;

/**
 * A {@link DocIdGenerator} that allows for object introspection to better determine what the docId should 
 * be for a given {@link Record}.
 */
public abstract class TypedDocIdGenerator<T> implements DocIdGenerator {

  @SuppressWarnings("unchecked")
  @Override
  public String getDocId(Object obj) {
    return getTypedDocId((T) obj);
  }

  /**
   * Return a unique identifier for {@link obj}
   * @param obj
   * @return
   */
  public abstract String getTypedDocId(T obj);

}
