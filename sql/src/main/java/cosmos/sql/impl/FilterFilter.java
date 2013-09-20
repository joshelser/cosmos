package cosmos.sql.impl;

import com.google.common.base.Predicate;
import com.google.gson.JsonSerializer;

import cosmos.sql.call.ChildVisitor;
import cosmos.sql.call.impl.FieldEquality;

public class FilterFilter implements Predicate<ChildVisitor> {
  
  @Override
  public boolean apply(ChildVisitor input) {
    if (input instanceof FieldEquality)
      return true;
    else
      return false;
  }
  
}
