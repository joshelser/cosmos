package cosmos.sql;

import net.hydromatic.optiq.jdbc.DriverVersion;

/**
 * 
 * Configurable based on the accumulo connector.
 * 
 */
public class CosmosJdbcDriverVersion extends DriverVersion {
  /** Creates an OptiqDriverVersion. */
  public CosmosJdbcDriverVersion(String configuredName) {
    super("Optiq JDBC Driver for Cosmos (" + configuredName + ")", "0.1", "Optiq-Cosmos-" + configuredName, "0.1",
    
    true, 0, 1, 0, 1);
  }
}
