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

import com.google.common.hash.PrimitiveSink;

/**
 * A pair contains two pronged tuple of children.
 * @param <T>
 * @param <K>
 */
public class Pair<T extends ChildVisitor,K extends ChildVisitor> extends ChildVisitor {
  
  private static final long serialVersionUID = 1L;
  ChildVisitor left = null;
  ChildVisitor right = null;
  
  public Pair(T left, K right) {
    this.left = left;
    this.right = right;
  }
  
  @Override
  public CallIfc<?> addChild(String id, ChildVisitor child) {
    super.addChild(id, child);
    return this;
  }
  
  public CallIfc<?> first() {
    return left;
  }
  
  public CallIfc<?> second() {
    return right;
  }
  
  @Override
  public void funnel(ChildVisitor from, PrimitiveSink into) {
    @SuppressWarnings("unchecked")
    Pair<ChildVisitor,ChildVisitor> child = (Pair<ChildVisitor,ChildVisitor>) from;
    child.left.funnel(child.left, into);
    child.right.funnel(child.right, into);
    
  }
  
}
