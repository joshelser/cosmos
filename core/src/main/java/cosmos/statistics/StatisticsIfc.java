package cosmos.statistics;

/**
 * Minimal selectivity derived from a given function.
 */
public interface StatisticsIfc{

	/**
	 * Determines if a plan should delay based on 
	 * schema statistics
	 * @return
	 */
	public boolean isDelayed(); 
	
	/**
	 * Returns the selectivity of this given item.
	 * @param fx
	 * @return
	 */
	public double selectivity();
	
	
}
