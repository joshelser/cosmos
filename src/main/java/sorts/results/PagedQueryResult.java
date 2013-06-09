package sorts.results;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.List;

import sorts.options.Paging;

import com.google.common.collect.Iterables;

public class PagedQueryResult<T> implements Results<List<T>> {

  protected final CloseableIterable<T> source;
  protected final Iterable<List<T>> pagedLimitedResults;
  
  public static <T> PagedQueryResult<T> create(CloseableIterable<T> results, Paging limits) {
    checkNotNull(results);
    checkNotNull(limits);
    return new PagedQueryResult<T>(results, limits);
  }
  
  public PagedQueryResult(CloseableIterable<T> results, Paging limits) {
    source = results;
    pagedLimitedResults = Iterables.partition(Iterables.limit(results, limits.maxResults().intValue()), limits.pageSize());
  }
  
  @Override
  public Iterator<List<T>> iterator() {
    return pagedLimitedResults.iterator();
  }
  
  @Override
  public void close() {
    this.source.close();
  }
}
