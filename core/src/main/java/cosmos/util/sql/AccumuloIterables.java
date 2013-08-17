package cosmos.util.sql;

import java.util.Iterator;

import com.google.common.collect.Iterators;

public class AccumuloIterables<T> implements Iterable<T> {

	Iterator<T> kvIter;

	public AccumuloIterables() {
		kvIter = Iterators.emptyIterator();
	}

	public AccumuloIterables(Iterator<T> uter) {
		kvIter = uter;
		
		System.out.println("your results====");
		while(kvIter.hasNext())
		{
			System.out.println(kvIter.next());
		}
	}

	@Override
	public Iterator<T> iterator() {
		
		return kvIter;
	}

}
