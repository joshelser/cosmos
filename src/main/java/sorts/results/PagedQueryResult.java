package sorts.results;

import java.util.Iterator;
import java.util.List;

import sorts.options.Paging;

import com.google.common.collect.Iterables;

public class PagedQueryResult<T> implements Iterator<List<T>>{

  protected final Iterator<List<T>> pagedLimitedResults;
  
  public static <T> PagedQueryResult<T> create(Iterable<T> results, Paging limits) {
    return new PagedQueryResult<T>(results, limits);
  }
  
  public PagedQueryResult(Iterable<T> results, Paging limits) {
    pagedLimitedResults = Iterables.partition(Iterables.limit(results, limits.maxResults().intValue()), limits.pageSize()).iterator();
  }

  @Override
  public boolean hasNext() {
    return pagedLimitedResults.hasNext();
  }

  @Override
  public List<T> next() {
    return pagedLimitedResults.next();
  }

  @Override
  public void remove() {
    pagedLimitedResults.remove();
  }
}
