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
package cosmos.sql.call.impl;

import java.util.Collection;

import com.google.common.base.Preconditions;

import cosmos.sql.call.CallIfc;
import cosmos.sql.call.ChildVisitor;
import cosmos.sql.call.Field;
import cosmos.sql.call.Literal;
import cosmos.sql.call.Pair;

/**
 * Field equality is an operator whose LHS is known to be a field and the RHS is known to be a literal
 */
public class FieldEquality extends Operator {

  private static final long serialVersionUID = 1L;

  public FieldEquality(CallIfc<?> left, CallIfc<?> right) {
    Preconditions.checkArgument(left instanceof Field);
    Preconditions.checkArgument(right instanceof Literal);
    addChild((Field) left, (Literal) right);

  }

  public FieldEquality(Field left, Literal right) {
    addChild(left, right);
  }

  private void addChild(Field left, Literal right) {
    Pair<Field,Literal> childPair = new Pair<Field,Literal>(left, right);
    addChild(childPair.getClass().getSimpleName(), childPair);
  }

  public Collection<ChildVisitor> getChildren() {
    return children.get(Pair.class.getSimpleName());
  }

}
