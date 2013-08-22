package cosmos.util.sql;

import java.util.Comparator;
import java.util.List;

public class Column implements Comparator<Object>{

	public List<Column> columns;


	public Column(String blah)
	{
		
	}

	@Override
	public int compare(Object o1, Object o2) {
		System.out.println("blahblahblah");
		return 0;
	}
}
