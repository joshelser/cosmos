package cosmos.sql.call.impl;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import cosmos.sql.call.CallIfc;
import cosmos.sql.call.BaseVisitor;
import cosmos.sql.call.ChildVisitor;

public class Filter extends BaseVisitor {
  
  public Filter() {
    
  }
  
  @Override
  public CallIfc addChild(String id, CallIfc child) {
    System.out.println("child is " + child.getClass().getCanonicalName());
    Preconditions.checkArgument(child instanceof ChildVisitor);
    return super.addChild(id, child);
  }
  
  public List<ChildVisitor> getFilters() {
    return Lists.newArrayList(Iterables.transform(children.values(), new Function<CallIfc,ChildVisitor>() {
      
      @Override
      public ChildVisitor apply(CallIfc child) {
        return (ChildVisitor) child;
      }
      
    }));
  }
}
