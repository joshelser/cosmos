package cosmos.sql;

import cosmos.statistics.StatisticsIfc;

public class QuerySelectivity implements StatisticsIfc {

	private String query;

	public QuerySelectivity(String query)
	{
		this.query = query;
	}
	
	
	@Override
	public boolean isDelayed() {
		return false;
	}

	@Override
	public double selectivity() {
		// TODO Auto-generated method stub
		return 0;
	}

}
