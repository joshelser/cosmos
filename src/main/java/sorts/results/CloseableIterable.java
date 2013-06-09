package sorts.results;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * 
 */
public class CloseableIterable<T> implements Results<T> {
  
  private final BatchScanner bs;
  private final Iterable<T> iterable;
  
  public CloseableIterable(BatchScanner bs, Iterable<T> iterable) {
    checkNotNull(bs);
    checkNotNull(iterable);
    
    this.bs = bs;
    this.iterable = iterable;
  }
  
  public static <T> CloseableIterable<T> create(BatchScanner bs, Iterable<T> iterable) {
    return new CloseableIterable<T>(bs, iterable);
  }
  
  public static <T> CloseableIterable<T> transform(BatchScanner bs, Function<Entry<Key,Value>,T> func) {
    return new CloseableIterable<T>(bs, Iterables.transform(bs, func));
  }
  
  public static <T> CloseableIterable<T> filterAndTransform(BatchScanner bs, Predicate<Entry<Key,Value>> filter, Function<Entry<Key,Value>,T> func) {
    return new CloseableIterable<T>(bs, Iterables.transform(Iterables.filter(bs, filter), func));
  }
  
  @Override
  public Iterator<T> iterator() {
    return iterable.iterator();
  }
  
  @Override
  public void close() {
    bs.close();
  }
  
}
