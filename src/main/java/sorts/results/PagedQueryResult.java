package sorts.results;

import java.util.Iterator;
import java.util.List;

import sorts.options.Paging;
import sorts.results.impl.MultimapQueryResult;

import com.google.common.collect.Iterables;

public class PagedQueryResult implements Iterator<List<MultimapQueryResult>>{

  protected final Iterator<List<MultimapQueryResult>> pagedLimitedResults;
  
  public PagedQueryResult(Iterable<MultimapQueryResult> results, Paging paging) {
    pagedLimitedResults = Iterables.partition(Iterables.limit(results, paging.maxResults().intValue()), paging.pageSize()).iterator();
  }

  @Override
  public boolean hasNext() {
    return pagedLimitedResults.hasNext();
  }

  @Override
  public List<MultimapQueryResult> next() {
    return pagedLimitedResults.next();
  }

  @Override
  public void remove() {
    pagedLimitedResults.remove();
  }
}
