package sorts.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 
 */
public class Single<T> implements Iterable<T> {
  protected final T single;
  
  public Single(T single) {
    checkNotNull(single);
    this.single = single;
  }
  
  public static <T> Single<T> create(T single) {
    return new Single<T>(single);
  }
  
  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      boolean hasNext = true;
      
      @Override
      public boolean hasNext() {
        return hasNext;
      }

      @Override
      public T next() {
        hasNext = false;
        return single;
      }

      @Override
      public void remove() {
        if (!hasNext) {
          throw new NoSuchElementException();
        }
        
        hasNext = false;
      }
      
    };
  }
  
}
