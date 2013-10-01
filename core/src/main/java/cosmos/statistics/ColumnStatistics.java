package cosmos.statistics;

import cosmos.results.Column;
import cosmos.store.Store;

public class ColumnStatistics implements IndexSelectivity{

	ColumnStatistics(Store store, Column column)
	{

	}
	
	@Override
	public boolean isDelayed() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public double selectivity() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long cardinality() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long countEstimate() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public String toString()
	{
		
	}

}
