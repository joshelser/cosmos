package sorts.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * 
 */
public class IdentitySet<T> implements Set<T> {
  
  @SuppressWarnings("rawtypes")
  private static final IdentitySet instance = new IdentitySet();
  
  @SuppressWarnings("unchecked")
  public static <T> IdentitySet<T> create() {
    return instance;
  }
  
  public IdentitySet() { }

  @Override
  public int size() {
    return Integer.MAX_VALUE;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean contains(Object o) {
    return true;
  }

  @Override
  public Iterator<T> iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("hiding")
  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean add(T e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }
  
}
