package cosmos.statistics;

import cosmos.options.Index;
import cosmos.statistics.store.Cardinality;
import cosmos.statistics.store.Count;
import cosmos.store.Store;

public class IndexStatistics extends ColumnStatistics{

	protected Count count;
	
	protected Cardinality cardinality;
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
		return Math.ceil( cardinality().get() / countEstimate().get() * 100 ); 
	}
	
	public void setCardinality(Cardinality  cardinality)
	{
		this.cardinality = cardinality;
	}

	@Override
	public Cardinality cardinality() {
		return cardinality;
	}
	
	public void setCountEstimate(Count estimate)
	{
		this.count = estimate;
	}

	@Override
	public Count countEstimate() {
		return count;
	}
	
	public String toString()
	{
		return null;
	}

}
