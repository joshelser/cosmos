package cosmos.statistics.store;

/**
 * Identifies a basic count.
 */
public class Count extends Statistic<Count> {

  public Count(final long count) {

    value += count;
  }

  public Count(final String json) {
    super(json);
  }

  public static void main(String[] args) {
    Count countOne = new Count(4);

    System.out.println(countOne.toString());
    Count count = new Count(countOne.toString());

    System.out.println(count.toString());
    
  }

}
