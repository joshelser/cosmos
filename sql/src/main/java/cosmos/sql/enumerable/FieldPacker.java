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
package cosmos.sql.enumerable;

import net.hydromatic.optiq.rules.java.EnumerableConvention;

import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexMultisetUtil;
import org.eigenbase.rex.RexProgram;

import cosmos.sql.TableScanner;

/**
 * Creates a reference to the table scanner and provides it the list of fields that the user wants while we're parsing the SQL expression
 * 
 * 
 */
public class FieldPacker extends ConverterRule {
  private TableScanner scanner;

  public FieldPacker(TableScanner scanner) {
    super(CalcRel.class, Convention.NONE, EnumerableConvention.INSTANCE, "FieldPacker");
    this.scanner = scanner;
  }

  public RelNode convert(RelNode rel) {

    final CalcRel calc = (CalcRel) rel;

    final RexProgram program = calc.getProgram();

    RelDataType rowType = program.getOutputRowType();

    scanner.setFields(rowType.getFieldNames());

    if (RexMultisetUtil.containsMultiset(program) || program.containsAggs()) {
      return null;
    }

    return new FieldPackerRelation(rel.getCluster(), rel.getTraitSet().replace(EnumerableConvention.INSTANCE), convert(calc.getChild(), calc.getChild()
        .getTraitSet().replace(EnumerableConvention.INSTANCE)), program, ProjectRelBase.Flags.Boxed);
  }
}
