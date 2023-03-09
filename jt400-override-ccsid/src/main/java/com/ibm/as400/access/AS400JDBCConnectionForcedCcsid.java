package com.ibm.as400.access;

import java.sql.SQLException;
import java.util.Properties;

public class AS400JDBCConnectionForcedCcsid extends AS400JDBCConnectionImpl {
	
	private static final String FORCED_CCSID="forced_ccsid";
	private Integer forcedCcsid;
	
	
	public AS400JDBCConnectionForcedCcsid() {
		super();
	}

	@Override
	public ConvTable getConverter(int ccsid) throws SQLException {
		if (this.forcedCcsid != null) {
			return super.getConverter(this.forcedCcsid);
		}
		return super.getConverter(ccsid);
	}
	
	@Override
    public void setProperties(JDDataSourceURL dataSourceUrl, JDProperties properties,
            AS400 as400, Properties info) throws SQLException {
		super.setProperties(dataSourceUrl, properties, as400, info);
		String p = info.getProperty(FORCED_CCSID);
		if (p != null) {
			forcedCcsid = Integer.parseInt(p);
		}
	}
}
