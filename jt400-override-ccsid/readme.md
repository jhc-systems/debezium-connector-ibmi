
# Purpose

Override the ccsid and force the character encoding

Incorrect setting of the QCCSID https://www.ibm.com/support/pages/what-impact-changing-qccsid-shipped-65535-another-ccsid can result in the wrong encoding of data in tables
If this is set to 65535 (binary the default), it indicates that jobs will have no CCSID, and no conversion will be performed.

Really you should not need this and just set the right encoding for QCCSID and on the tables. 
Sadly if you have a large legacy code base and a large number of customers who have this incorrectly configured that correct solution might be costly and bring significant risks

Note the original AS400JDBCDriver has a static block that on class load registers the driver so it's really important to unregister it before registering the new driver the static methods in AS400JDBCDriverRegistration do this for you

# Usage


```
AS400JDBCDriverRegistration.registerCcsidDriver();

Properties props = new Properties();
props.setProperty("from_ccsid", 37);
props.setProperty("to_ccsid", 37);
props.setProperty("user", "user");
props.setProperty("password", "password");
try (Connection con = DriverManager.getConnection("jdbc:as400://db/;naming=system;prompt=false;libraries=xxx;errors=full", props)) {
```

# Implementation details

AS400JDBCDriver

AS400JDBCPreparedStatementImpl calls commonExecuteBefore which looks up the table details in the cache and in the parameterRow_ there is the ccsid, this is passed to the connection method getConverter for the character converter.
getConverter is where we inject our own implementation that ignores the correct database value and uses the one configured

The annoying thing is injecting in our Connection which looks requires copying the entire AS400JDBCDriver and just replacing the defaultImpl in prepareConnection to use our new instance. We also have to change the registration part which is in a static block so it actually registers our Driver