package cosmos.statistics;

import cosmos.options.Index;
import cosmos.store.Store;

public class IndexStatistics extends ColumnStatistics{

	protected long count;
	
	protected long cardinality;
	// like we are not indexed
	protected boolean hasStats;
	IndexStatistics(Store store, Index index)
	{
		super(store,index.column());
	}
	
	@Override
	public boolean isDelayed() {
		return hasStats;
	}

	@Override
	public double selectivity() {
		// remember your rules of math ... / then * ;)
		return Math.ceil( cardinality() / countEstimate() * 100 ); 
	}
	
	public void setCardinality(long cardinality)
	{
		this.cardinality = cardinality;
	}

	@Override
	public long cardinality() {
		return cardinality;
	}
	
	public void setCountEstimate(long estimate)
	{
		this.count = estimate;
	}

	@Override
	public long countEstimate() {
		return count;
	}
	
	public String toString()
	{
		return null;
	}

}
