package cosmos.statistics.store;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * This can be treated as a counter -- rough implementation
 * of hyper log log
 */
public class Cardinality extends Statistic<Cardinality> {

  private int p; // p E [4..16] so they say or some stuff

  private int m;

  private double alphaSubM;

  private int[] registers;

  private static final HashFunction hf = Hashing.murmur3_128(323);

  public boolean useHyperLogLog = false;

  public Cardinality(Integer p) {
    if (p < 4 || p > 16) {
      throw new IllegalArgumentException("p must be between 4 and 16, inclusively");
    }
    this.p = p;
    this.m = 1 << p; // 2 ^p
    switch (p) {
      case 4:
        alphaSubM = 0.673;
        break;
      case 5:
        alphaSubM = 0.697;
        break;
      case 6:
        alphaSubM = 0.709;
        break;
      default:
        alphaSubM = 0.7213 / (1 + 1.079 / m);
    }

    registers = new int[m];

    useHyperLogLog = true;
  }

  public Cardinality(long cardinality) {
	this(Integer.valueOf(4));
    value += cardinality;
  }

  public Cardinality(String json) {
    super(json);
  }

  public void update(String term) {
	  Preconditions.checkNotNull("Incoming term should not be null", term);
    final int x = hf.hashBytes(term.getBytes()).asInt();
    int j = x >>> (Integer.SIZE - p);
    int leftMost = (x << p) | (1 << (p - 1)) + 1;
    if (leftMost == 0)
      leftMost = 0;
    else {
      leftMost = 1 + Integer.numberOfLeadingZeros(leftMost);
    }

    registers[j] = Math.max(registers[j], leftMost);
  }

  @Override
  public double get() {
    if (!useHyperLogLog)
      return super.get();
    else {
      // raw estimate
      double V = 0.0;
      for (int i = 0; i < m; ++i) {
        V += 1.0 / (1 << registers[i]);
      }
      double E = alphaSubM * m * m / V;

      if (E <= (5.0 / 2.0) * m) {
        // small range correction ( according to page 3
        // i don't really know what's going on )
        int corr = 0;
        for (int i = 0; i < m; i++) {
          if (registers[i] == 0)
            corr++;
        }
        if (corr == 0) {
          return (double) V;
        } else {
          // // linear counting m log(m/V)
          return (double) ((double) m * Math.log((double) m / (double) corr));
        }

      } else if (V <= 1L << 32 / 30) {
        return (double) V;
      } else {
        // this is the large range correction they were talking about, eh?
        return (double) (-1 * (1L << 32)) * (double) Math.log(1.0 - V / (1L << 32));
      }

    }
  }

}
