package cosmos.util.sql;

import net.hydromatic.optiq.jdbc.DriverVersion;

public class AccumuloJdbcDriverVersion extends DriverVersion {
	  /** Creates an OptiqDriverVersion. */
	public AccumuloJdbcDriverVersion() {
	    super(
	        "Optiq JDBC Driver for Accumulo",
	        "0.1",
	        "Optiq-Accumulo",
	        "0.1",

	        true,
	        0,
	        1,
	        0,
	        1);
	  }
	}