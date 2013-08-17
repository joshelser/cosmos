package cosmos.util.sql;

import net.hydromatic.optiq.jdbc.DriverVersion;

class AccumuloJdbcDriverVersion extends DriverVersion {
	  /** Creates an OptiqDriverVersion. */
	AccumuloJdbcDriverVersion() {
	    super(
	        "Optiq JDBC Driver for Splunk",
	        "0.2",
	        "Optiq-Splunk",
	        "0.2",
	        true,
	        0,
	        1,
	        0,
	        1);
	  }
	}