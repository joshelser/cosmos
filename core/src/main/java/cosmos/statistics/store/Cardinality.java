package cosmos.statistics.store;

public class Cardinality extends Statistic<Cardinality> {

	public Cardinality(long cardinality) {
		value += cardinality;
	}

}
