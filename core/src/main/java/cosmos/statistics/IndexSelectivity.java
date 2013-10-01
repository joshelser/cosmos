package cosmos.statistics;

import cosmos.statistics.store.Cardinality;
import cosmos.statistics.store.Count;

public interface IndexSelectivity extends StatisticsIfc {

	/**
	 * Cardinality of the statistic
	 * @return
	 */
	public Cardinality cardinality();
	
	/**
	 * Estimated result count
	 * @return
	 */
	public Count countEstimate();
	
}
