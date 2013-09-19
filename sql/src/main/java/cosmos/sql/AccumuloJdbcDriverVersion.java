package cosmos.sql;

import net.hydromatic.optiq.jdbc.DriverVersion;

/**
 * 
 * Configurable based on the accumulo connector.
 *
 */
public class AccumuloJdbcDriverVersion extends DriverVersion {
	  /** Creates an OptiqDriverVersion. */
	public AccumuloJdbcDriverVersion(String configuredName) {
	    super(
	        "Optiq JDBC Driver for Accumulo (" + configuredName + ")",
	        "0.1",
	        "Optiq-Accumulo-" + configuredName,
	        "0.1",

	        true,
	        0,
	        1,
	        0,
	        1);
	  }
	}