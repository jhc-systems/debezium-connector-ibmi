package com.fnz.db2.journal.retrieve;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fnz.db2.journal.data.types.AS400VarBin;
import com.fnz.db2.journal.data.types.AS400VarChar;
import com.fnz.db2.journal.data.types.AS400Xml;
import com.fnz.db2.journal.retrieve.SchemaCacheIF.Structure;
import com.fnz.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.ibm.as400.access.AS400Bin2;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400Bin8;
import com.ibm.as400.access.AS400ByteArray;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Date;
import com.ibm.as400.access.AS400Float4;
import com.ibm.as400.access.AS400Float8;
import com.ibm.as400.access.AS400PackedDecimal;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400Time;
import com.ibm.as400.access.AS400Timestamp;
import com.ibm.as400.access.AS400ZonedDecimal;

public class JdbcFileDecoder extends JournalFileEntryDecoder {

    private static final AS400Float8 AS400_FLOAT8 = new AS400Float8();
    private static final AS400Float4 AS400_FLOAT4 = new AS400Float4();
    private static final AS400Time AS400_TIME = new AS400Time();
    private static final AS400Date AS400_DATE = new AS400Date();
    private static final AS400Timestamp AS400_TIMESTAMP = new AS400Timestamp();
    private static final AS400Xml AS400_XML = new AS400Xml();
    private static final AS400Bin8 AS400_BIN8 = new AS400Bin8();
    private static final AS400Bin4 AS400_BIN4 = new AS400Bin4();
    private static final AS400Bin2 AS400_BIN2 = new AS400Bin2();
    private static final String GET_DATABASE_NAME = "values ( CURRENT_SERVER )"; 
    private static final String UNIQUE_KEYS = "SELECT DBKFLD FROM QSYS.QADBKATR WHERE dbklib=? AND dbkfil=? ORDER BY DBKORD ASC"; // find the short name for keys
    
	private final Connect<Connection, SQLException> jdbcConnect;
	private final String databaseName;
	private final SchemaCacheIF schemaCache;
	private final int forcedCcsid;
	
	public JdbcFileDecoder(Connect<Connection, SQLException> con, String database, SchemaCacheIF schemaCache, Integer forcedCcsid) {
		super();
		this.jdbcConnect = con;
		this.schemaCache = schemaCache;
        this.databaseName = database;
        this.forcedCcsid = (forcedCcsid == null) ? -1: forcedCcsid ;
	}

	/* https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/apis/QJORJRNE.htm
		This journal entry's entry specific data
		Offset 	Type 	Field
		Dec 	Hex
		0 	0 	CHAR(5) 	Length of entry specific data
		5 	5 	CHAR(11) 	Reserved
		16 	16 	CHAR(*) 	Entry specific data
	 */
	private static AS400Text LENGTH_DECODER = new AS400Text(5);
	private static Object[] EMPTY = new Object[] {};
	@Override
	public Object[] decodeFile(EntryHeader entryHeader, byte[] data, int offset) throws Exception {
		Optional<TableInfo> tableInfoOpt = getRecordFormat(entryHeader.getFile(), entryHeader.getLibrary());
		
		return tableInfoOpt.map(tableInfo -> {
			String lengthStr = (String)LENGTH_DECODER.toObject(data, offset + entryHeader.getEntrySpecificDataOffset());
			int length = Integer.parseInt(lengthStr);
			if (length > 0) {
				Object[] os = decodeEntry(tableInfo.getAs400Structure(), data, offset + entryHeader.getEntrySpecificDataOffset() + ENTRY_SPECIFIC_DATA_OFFSET);
				return os;
			} else {
				log.error("Empty journal entry for {}.{} is (before image) journalling set corretly for this table?", entryHeader.getLibrary() , entryHeader.getFile());
				return EMPTY;
			}
		}).orElse(EMPTY);
	}
	
	public Object[] decodeEntry(AS400Structure entryDetailStructure , byte[] data, int offset) {
		Object[] result = (Object[]) entryDetailStructure.toObject(data, offset);
		return result;
	}
	
	public static String getDatabaseName(Connection con) throws SQLException {

		PreparedStatement st = con.prepareStatement(GET_DATABASE_NAME);
		ResultSet rs = st.executeQuery();
		if (rs.next())
			return StringHelpers.safeTrim(rs.getString(1));
		else
			return "";
        // debezium doesn't close connections
	}
	
	public void clearCache(String systemTableName, String schema) {
		String longTableName = getLongName(schema, systemTableName).orElse(systemTableName);
		schemaCache.clearCache(databaseName, schema, longTableName);
	}
	
	/**
	 * @see io.debezium.jdbc.JdbcConnection.readTableColumn
	 * @param table
	 * @return
	 * @throws Exception
	 * 
	 * note that some tables (e.g. INDICIA2) are created and deleted during workflow so may not exist
	 */
	public Optional<TableInfo> getRecordFormat(String systemTableName, String schema) {
		String longTableName = getLongName(schema, systemTableName).orElse(systemTableName);

		try {
			TableInfo tableInfo = schemaCache.retrieve(databaseName, schema, longTableName);
			if (tableInfo != null) {
				return Optional.of(tableInfo);
			}
			
			log.info("missed cache fetching structure for {} {}", schema, systemTableName);

			String databaseCatalog = null;
			List<AS400DataType> as400structure = new ArrayList<>();
			List<Structure> jdbcStructure = new ArrayList<>();

			Connection con = jdbcConnect.connection();
			DatabaseMetaData metadata = con.getMetaData();
			try (ResultSet columnMetadata = metadata.getColumns(databaseCatalog, schema, longTableName, null)) {
				while (columnMetadata.next()) {
					// @see
					// https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html#getColumns(java.lang.String,java.lang.String,java.lang.String,java.lang.String)
					String name = columnMetadata.getString(4);
					String type = columnMetadata.getString(6);
					int precision = columnMetadata.getInt(9);
					int length = columnMetadata.getInt(7);
					int jdcbType = columnMetadata.getInt(5);

					boolean optional = isNullable(columnMetadata.getInt(11));
					int position = columnMetadata.getInt(17);
					boolean autoInc = "YES".equalsIgnoreCase(columnMetadata.getString(23));

					jdbcStructure.add(
							new Structure(name, type, jdcbType, length, precision, optional, position, autoInc));
					AS400DataType dataType = toDataType(schema, longTableName, name, type, length, precision);

					as400structure.add(dataType);
				}
				AS400Structure entryDetailStructure = new AS400Structure(
						as400structure.toArray(new AS400DataType[as400structure.size()]));

				List<String> primaryKeys = primaryKeysFromMeta(longTableName, schema, databaseCatalog, metadata);

				if (primaryKeys.isEmpty()) {
					primaryKeys = ddsPrimaryKeys(systemTableName, schema);
				}

				tableInfo = new TableInfo(jdbcStructure, primaryKeys, entryDetailStructure);
				schemaCache.store(databaseName, schema, longTableName, tableInfo);

				return Optional.of(tableInfo);
			}
		} catch (Exception e) {
			log.error("Failed to retrieve table info for {} {}", schema, longTableName, e);
		}
        log.warn("No table structure found for {}", systemTableName);

		return Optional.empty();
	}

	private List<String> ddsPrimaryKeys(String table, String schema) throws SQLException {
		List<String> primaryKeys =  new ArrayList<>();
		Connection con = jdbcConnect.connection();

		try (PreparedStatement ps = con.prepareStatement(UNIQUE_KEYS)) {
			ps.setString(1, schema);
			ps.setString(2, table);
			try (ResultSet rs = ps.executeQuery()) {
				while(rs.next()) {
			        String columnName = StringHelpers.safeTrim(rs.getString(1));
			        primaryKeys.add(columnName);
				}
			}
		}
		return primaryKeys;
	}

	private List<String> primaryKeysFromMeta(String table, String schema, String databaseCatalog, DatabaseMetaData metadata) throws SQLException {
		List<String> primaryKeys =  new ArrayList<>();
		try (ResultSet rs = metadata.getPrimaryKeys(databaseCatalog, schema, table)) {
            while (rs.next()) {
                String columnName = StringHelpers.safeTrim(rs.getString(4));
                primaryKeys.add(columnName);
            }
        }
		return primaryKeys;
	}
	
    
    //from debezium
    static boolean isNullable(int jdbcNullable) {
        return jdbcNullable == ResultSetMetaData.columnNullable || jdbcNullable == ResultSetMetaData.columnNullableUnknown;
    }
    
    private static final String GET_TABLE_NAME = "select table_name from qsys2.systables where table_schema=? AND system_table_name=?";
    private final Map<String, Optional<String>> systemToLongName = new HashMap<>();
    public Optional<String> getLongName(String schemaName, String systemName) {
        if (systemToLongName.containsKey(systemName)) {
            return systemToLongName.get(systemName);
        }
        else {
        	try {
	    		Connection con = jdbcConnect.connection();
	        	try (PreparedStatement ps = con.prepareStatement(GET_TABLE_NAME)) {
	        		ps.setString(1, schemaName);
	        		ps.setString(2, systemName);
	        		try (ResultSet rs = ps.executeQuery()) {
	        			if (rs.next()) {
	    				Optional<String> longTableName = Optional.of(StringHelpers.safeTrim(rs.getString(1)));
			            systemToLongName.put(systemName, longTableName);
			            return longTableName;
	        			}
	        		}
	        	}
        	} catch (Exception e) {
        		log.error("failed looking up long table name", e);
        	}
		}
        log.warn("No long table name found for {}", systemName);
        systemToLongName.put(systemName, Optional.<String>empty());
        return Optional.<String>empty();
    }
	
    static final Pattern BIT_DATA = Pattern.compile("CHAR \\(([(0-9]*)\\) FOR BIT DATA");
    static final Pattern VAR_BIT_DATA = Pattern.compile("VARCHAR \\(([(0-9]*)\\) FOR BIT DATA");
    
    private static final String GET_CCSID = "select table_name, system_table_name, column_name, system_column_name, ccsid FROM qsys2.SYSCOLUMNS where table_schema=? and (system_table_name = ? or table_name = ?)";
	private final Map<String, Integer> ccsidMap = new HashMap<>();
	
    public Integer getCcsid(String schema, String table, String columnName) {
    	if (forcedCcsid != -1) {
    		return forcedCcsid;
    	}
    	
    	String canonicalName = String.format("%s.%s.%s", schema, table, columnName);
    	if (ccsidMap.containsKey(canonicalName)) {
    		return ccsidMap.get(canonicalName);
    	}
    	
		try {
			fetchAllCcsidForTable(schema, table);
			
			return ccsidMap.get(canonicalName);
		} catch (SQLException e) {
			log.error("failed to fetch ccsid", e);
			return -1;
		}
    }
    
    private void fetchAllCcsidForTable(String schema, String table) throws SQLException {
		Connection con = jdbcConnect.connection();
    	try (PreparedStatement ps = con.prepareStatement(GET_CCSID)) {
    		ps.setString(1, schema.toUpperCase());
    		ps.setString(2, table.toUpperCase());
    		ps.setString(3, table.toUpperCase());
    		try (ResultSet rs = ps.executeQuery()) {
    			while (rs.next()) {
					String longTableName = StringHelpers.safeTrim(rs.getString(1));
					String shortTableName = StringHelpers.safeTrim(rs.getString(2));
					String longcolumn = StringHelpers.safeTrim(rs.getString(3));
					String shortcolumn = StringHelpers.safeTrim(rs.getString(4));
					Object ccsidObj = rs.getObject(5);
					String canonicalLongName = String.format("%s.%s.%s", schema, longTableName, longcolumn);
					String canonicalShortName = String.format("%s.%s.%s", schema, shortTableName, shortcolumn);
					int ccsid = (ccsidObj == null) ? -1 : (Integer)ccsidObj;
					
		            ccsidMap.put(canonicalLongName, ccsid);
		            ccsidMap.put(canonicalShortName, ccsid);
	    		}
	    	}
    	}
    }
    
    AS400Text getText(int length, int ccsid) {
    	if (forcedCcsid != -1) {
    		return new AS400Text(length, forcedCcsid);
    	} else if (ccsid != -1) {
    		return new AS400Text(length, ccsid);
    	}
    	return new AS400Text(length);
    }

    AS400VarChar getVarText(int length, Integer ccsid) {
    	if (forcedCcsid != -1) {
    		return new AS400VarChar(length, ccsid);
    	}
    	return new AS400VarChar(length);
    }
	public AS400DataType toDataType(String schema, String table, String columnName, String type, int length, Integer precision) {
		switch (type) {
			case "DECIMAL":
				return new AS400PackedDecimal(length, precision);
			case "CHAR () FOR BIT DATA": // password fields - treat as binary
				return new AS400ByteArray(length);
			case "VARCHAR () FOR BIT DATA": // password fields - treat as binary
				return new AS400VarBin(length);
			case "CHAR": 
				return getText(length, getCcsid(schema, table, columnName));
			case "NCHAR": 
				return getText(length, getCcsid(schema, table, columnName));
			case "NVARCHAR": 
				return getVarText(length, getCcsid(schema, table, columnName));
			case "TIMESTAMP":
				return AS400_TIMESTAMP;
			case "VARCHAR":
				return getVarText(length, getCcsid(schema, table, columnName));
			case "NUMERIC":
				return new AS400ZonedDecimal(length, precision);
			case "DATE":
				return AS400_DATE;
			case "TIME":
				return AS400_TIME;		
			case "REAL":
				return AS400_FLOAT4;	
			case "DOUBLE":
				return AS400_FLOAT8;
			case "SMALLINT":
				return AS400_BIN2;
			case "INTEGER":
				return AS400_BIN4;
			case "BIGINT":
				return AS400_BIN8;
			case "BINARY":
				return new AS400ByteArray(length);
			case "VARBINARY":
				return new AS400VarBin(length);
			case "XML":
				return AS400_XML;
//			case "CLOB": 
//				return new AS400Clob(as400); 
			default:
				Optional<Integer> varLength = bitDataLengthFromRegex(type, length, VAR_BIT_DATA);
				if (varLength.isPresent()) {
					return new AS400VarBin(varLength.get());
				}
				Optional<Integer> fixedLenght = bitDataLengthFromRegex(type, length, BIT_DATA);
				if (fixedLenght.isPresent()) {
					return new AS400ByteArray(fixedLenght.get());
				}
		}
		throw new IllegalArgumentException(String.format("Unsupported type %s for column %s", type, columnName));
	}

	private  Optional<Integer> bitDataLengthFromRegex(String type, int length, Pattern regex) {
		Matcher matcher = regex.matcher(type); //  - treat as binary
		if (matcher.matches()) {
			String size = matcher.group(1);
			if (size.isEmpty())
				return  Optional.of(length);
			else {
				int l = Integer.parseInt(size);
				return Optional.of(l);
			}
		}
		return Optional.empty();		
	}
}
