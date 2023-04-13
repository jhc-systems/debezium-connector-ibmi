package com.ibm.as400.access;

import java.lang.StackWalker.StackFrame;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AS400JDBCConnectionForcedCcsid extends AS400JDBCConnectionImpl {
	private static Logger log = Logger.getLogger(AS400JDBCConnectionForcedCcsid.class.toString());
	
	public static final String FORCED_CCSID="forced_ccsid";
	private Integer forcedCcsid;
	
	
	public AS400JDBCConnectionForcedCcsid() {
		super();
	}

	@Override
	public ConvTable getConverter(int ccsid) throws SQLException {
//		log.info(() -> String.format("requested ccsid %d\n\t%s", ccsid, getStack()));
		if (ccsid == 0 || ccsid == 1 || ccsid == 65535 || ccsid == -1) return converter_;
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
	
	public List<String> toString(Stream<StackFrame> stackFrameStream) {
		
	    return stackFrameStream.map(f -> String.format("%s.%s#%d", f.getClassName(), f.getMethodName(), f.getLineNumber())).collect(Collectors.toList());
	}
	
	private String getStack() {
	    List<String> stackTrace = StackWalker.getInstance()
	    	      .walk(this::toString);
	    return String.join("\n\t", stackTrace);
	}
}
