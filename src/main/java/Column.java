
public class Column {
  private final String column;
  
  public Column(String column) {
    this.column = column;
  }

  public String column() {
    return this.column;
  }
  
  public static Column create(String column) {
    return new Column(column);
  }
}
