package cosmos.sql.call;

import java.util.Collection;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.hash.Funnel;

public abstract class ChildVisitor extends BaseVisitor<ChildVisitor> implements Funnel<ChildVisitor> {
  
  public ChildVisitor() {}
  
  public Iterable<?> visit(final Function<ChildVisitor,Iterable<?>> callbackFunction, final Predicate<ChildVisitor> childFilter) {
    Collection<ChildVisitor> equalities = children.values();
    return Iterables.concat(Iterables.transform(Iterables.filter(equalities, childFilter), callbackFunction));
  }
  
}
