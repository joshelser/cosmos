package cosmos.sql.rules;

import java.sql.ResultSet;
import java.sql.Statement;

import net.hydromatic.optiq.Schema;

public interface ResultSetNode {

  public ResultSet execute(Statement statement, Schema parentSchema);
}