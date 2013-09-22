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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.hash.Funnel;

/**
 * Child visitor is a base visitor who also has a funnel to help define values to any possible sink.
 */
public abstract class ChildVisitor extends BaseVisitor<ChildVisitor> implements Funnel<ChildVisitor> {

  private static final long serialVersionUID = 1L;

  public ChildVisitor() {}

  public Iterable<?> visit(final Function<ChildVisitor,Iterable<?>> callbackFunction, final Predicate<ChildVisitor> childFilter) {
    Collection<ChildVisitor> equalities = children.values();
    return Iterables.concat(Iterables.transform(Iterables.filter(equalities, childFilter), callbackFunction));
  }

}
