package cosmos.statistics.store;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class Statistic<P extends Statistic<P>> {

  protected String name;

  protected double value;

  protected Statistic() {
    name = getClass().getSimpleName().toLowerCase();
  }

  protected Statistic(String json) {
    this();
    Gson gson = new Gson();
    Statistic stat = gson.fromJson(json, Statistic.class);
    name = stat.name;
    value = stat.value;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public double get() {
    return value;
  }

  public P aggregate(P stat) {
    value += stat.get();
    return (P) this;
  }

  public String store() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }

  @Override
  public String toString() {
    return store();
  }

}
