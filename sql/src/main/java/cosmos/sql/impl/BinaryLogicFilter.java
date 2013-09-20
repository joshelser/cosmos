package cosmos.sql.impl;

import com.google.common.base.Predicate;

import cosmos.sql.call.ChildVisitor;
import cosmos.sql.call.impl.operators.AndOperator;

public class BinaryLogicFilter implements Predicate<ChildVisitor> {
  
  @Override
  public boolean apply(ChildVisitor input) {
    if (input instanceof AndOperator) {
      return true;
    }
    return false;
  }
  
}
