package cosmos.statistics.store;


public abstract class Statistic<P extends Statistic<P>> {

	protected double value;

	public double get() {
		return value;
	}

	public P aggregate(P stat) {
		value += stat.get();

		return (P) this;
	}
	
	public String store()
	{
		
		return null;
	}
}
