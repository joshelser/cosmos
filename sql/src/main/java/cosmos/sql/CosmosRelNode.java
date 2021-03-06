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

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;

import cosmos.sql.call.BaseVisitor;
import cosmos.sql.call.CallIfc;

/**
 * Accumulo relational expression
 * 
 * @TODO implement stronger planning here
 */
public interface CosmosRelNode extends RelNode {
  /**
   * Calling convention for relational expressions that are "implemented" by generating Drill logical plans.
   */
  Convention CONVENTION = new Convention.Impl("COSMOS", CosmosRelNode.class);

  int implement(Plan implementor);

  class Plan {

    protected BaseVisitor<CallIfc<?>> operations;

    public DataTable<?> table;

    public Plan() {
      operations = new BaseVisitor<CallIfc<?>>();
    }

    /**
     * Convenience method
     * 
     * @param operation
     */
    public void add(String id, CallIfc<?> operation) {

      operations.addChild(id, operation);
    }

    public void visitChild(RelNode input) {
      ((CosmosRelNode) input).implement(this);

    }

    public BaseVisitor<CallIfc<?>> getChildren() {
      return operations;
    }
  }
}
