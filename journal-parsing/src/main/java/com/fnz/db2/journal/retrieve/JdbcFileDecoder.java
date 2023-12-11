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
	private static final String UNIQUE_KEYS = """
			SELECT c.column_name FROM qsys.QADBKATR k
			      INNER JOIN qsys2.SYSCOLUMNS c on c.table_schema=k.dbklib and c.system_table_name=k.dbkfil AND c.system_column_name=k.DBKFLD
			      WHERE k.dbklib=? AND k.dbkfil=? ORDER BY k.DBKPOS ASC
			     """;

	private final Connect<Connection, SQLException> jdbcConnect;
	private final String databaseName;
	private final SchemaCacheIF schemaCache;
	private final CcsidCache ccsidCache;
	private final BytesPerChar octetLengthCache;

	public JdbcFileDecoder(Connect<Connection, SQLException> con, String database, SchemaCacheIF schemaCache,
			Integer fromCcsid, Integer toCcsid) {
		super();
		this.jdbcConnect = con;
		this.schemaCache = schemaCache;
		this.databaseName = database;
		ccsidCache = new CcsidCache(con, fromCcsid, toCcsid);
		octetLengthCache = new BytesPerChar(con);
	}

	/*
	 * https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/apis/QJORJRNE.htm
	 * This journal entry's entry specific data Offset Type Field Dec Hex 0 0
	 * CHAR(5) Length of entry specific data 5 5 CHAR(11) Reserved 16 16 CHAR(*)
	 * Entry specific data
	 */
	private static final AS400Text LENGTH_DECODER = new AS400Text(5);
	private static final Object[] EMPTY = new Object[] {};

	@Override
	public Object[] decodeFile(EntryHeader entryHeader, byte[] data, int offset) throws Exception {
		final Optional<TableInfo> tableInfoOpt = getRecordFormat(entryHeader.getFile(), entryHeader.getLibrary());

		return tableInfoOpt.map(tableInfo -> {
			final String lengthStr = (String) LENGTH_DECODER.toObject(data,
					offset + entryHeader.getEntrySpecificDataOffset());
			final int length = Integer.parseInt(lengthStr);
			if (length > 0) {
				final Object[] os = decodeEntry(tableInfo.getAs400Structure(), data,
						offset + entryHeader.getEntrySpecificDataOffset() + ENTRY_SPECIFIC_DATA_OFFSET);
				return os;
			} else {
				log.error("Empty journal entry for {}.{} is (before image) journalling set corretly for this table?",
						entryHeader.getLibrary(), entryHeader.getFile());
				return EMPTY;
			}
		}).orElse(EMPTY);
	}

	public Object[] decodeEntry(AS400Structure entryDetailStructure, byte[] data, int offset) {
		final Object[] result = (Object[]) entryDetailStructure.toObject(data, offset);
		return result;
	}

	public static String getDatabaseName(Connection con) throws SQLException {
		try (PreparedStatement st = con.prepareStatement(GET_DATABASE_NAME)) {
			try (ResultSet rs = st.executeQuery()) {
				if (rs.next()) {
					return StringHelpers.safeTrim(rs.getString(1));
				} else {
					return "";
				}
			}
		}
		// debezium doesn't close connections
	}

	public void clearCache(String systemTableName, String schema) {
		final String longTableName = getLongName(schema, systemTableName).orElse(systemTableName);
		schemaCache.clearCache(databaseName, schema, longTableName);
	}

	/**
	 * @see io.debezium.jdbc.JdbcConnection.readTableColumn
	 * @param table
	 * @return
	 * @throws Exception
	 *
	 *                   note that some tables (e.g. INDICIA2) are created and
	 *                   deleted during workflow so may not exist
	 */
	public Optional<TableInfo> getRecordFormat(String systemTableName, String schema) {
		final String longTableName = getLongName(schema, systemTableName).orElse(systemTableName);

		try {
			TableInfo tableInfo = schemaCache.retrieve(databaseName, schema, longTableName);
			if (tableInfo != null) {
				return Optional.of(tableInfo);
			}

			log.info("missed cache fetching structure for {} {}", schema, systemTableName);

			final String databaseCatalog = null;
			final List<AS400DataType> as400structure = new ArrayList<>();
			final List<Structure> jdbcStructure = new ArrayList<>();

			final Connection con = jdbcConnect.connection();
			final DatabaseMetaData metadata = con.getMetaData();
			try (ResultSet columnMetadata = metadata.getColumns(databaseCatalog, schema, longTableName, null)) {
				while (columnMetadata.next()) {
					// @see
					// https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html#getColumns(java.lang.String,java.lang.String,java.lang.String,java.lang.String)
					final String name = columnMetadata.getString(4);
					final String type = columnMetadata.getString(6);
					final int precision = columnMetadata.getInt(9);
					final int length = columnMetadata.getInt(7);
					final int jdcbType = columnMetadata.getInt(5);

					final boolean optional = isNullable(columnMetadata.getInt(11));
					final int octectLength = columnMetadata.getInt(16);
					final int position = columnMetadata.getInt(17);
					final boolean autoInc = "YES".equalsIgnoreCase(columnMetadata.getString(23));

					jdbcStructure
					.add(new Structure(name, type, jdcbType, length, precision, optional,
							position, autoInc));
					final AS400DataType dataType = toDataType(schema, longTableName, name, type, length, precision);

					as400structure.add(dataType);
					octetLengthCache.add(schema, longTableName, name, length, octectLength);
				}
				final AS400Structure entryDetailStructure = new AS400Structure(
						as400structure.toArray(new AS400DataType[as400structure.size()]));

				List<String> primaryKeys = primaryKeysFromMeta(longTableName, schema, databaseCatalog, metadata);

				if (primaryKeys.isEmpty()) {
					primaryKeys = ddsPrimaryKeys(systemTableName, schema);
				}

				tableInfo = new TableInfo(jdbcStructure, primaryKeys, entryDetailStructure);
				schemaCache.store(databaseName, schema, longTableName, tableInfo);

				return Optional.of(tableInfo);
			}
		} catch (final Exception e) {
			log.error("Failed to retrieve table info for {} {}", schema, longTableName, e);
		}
		log.warn("No table structure found for {}", systemTableName);

		return Optional.empty();
	}


	private List<String> ddsPrimaryKeys(String table, String schema) throws SQLException {
		final List<String> primaryKeys = new ArrayList<>();
		final Connection con = jdbcConnect.connection();

		try (PreparedStatement ps = con.prepareStatement(UNIQUE_KEYS)) {
			ps.setString(1, schema);
			ps.setString(2, table);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					final String columnName = StringHelpers.safeTrim(rs.getString(1));
					primaryKeys.add(columnName);
				}
			}
		}
		return primaryKeys;
	}

	private List<String> primaryKeysFromMeta(String table, String schema, String databaseCatalog,
			DatabaseMetaData metadata) throws SQLException {
		final List<String> primaryKeys = new ArrayList<>();
		try (ResultSet rs = metadata.getPrimaryKeys(databaseCatalog, schema, table)) {
			while (rs.next()) {
				final String columnName = StringHelpers.safeTrim(rs.getString(4));
				primaryKeys.add(columnName);
			}
		}
		return primaryKeys;
	}

	// from debezium
	static boolean isNullable(int jdbcNullable) {
		return jdbcNullable == ResultSetMetaData.columnNullable
				|| jdbcNullable == ResultSetMetaData.columnNullableUnknown;
	}

	private static final String GET_TABLE_NAME = "select table_name from qsys2.systables where table_schema=? AND system_table_name=?";
	private final Map<String, Optional<String>> systemToLongName = new HashMap<>();

	public Optional<String> getLongName(String schemaName, String systemName) {
		if (systemToLongName.containsKey(systemName)) {
			return systemToLongName.get(systemName);
		} else {
			try {
				final Connection con = jdbcConnect.connection();
				try (PreparedStatement ps = con.prepareStatement(GET_TABLE_NAME)) {
					ps.setString(1, schemaName);
					ps.setString(2, systemName);
					try (ResultSet rs = ps.executeQuery()) {
						if (rs.next()) {
							final Optional<String> longTableName = Optional.of(StringHelpers.safeTrim(rs.getString(1)));
							systemToLongName.put(systemName, longTableName);
							return longTableName;
						}
					}
				}
			} catch (final Exception e) {
				log.error("failed looking up long table name", e);
			}
		}
		log.warn("No long table name found for {}", systemName);
		systemToLongName.put(systemName, Optional.<String>empty());
		return Optional.<String>empty();
	}

	static final Pattern BIT_DATA = Pattern.compile("CHAR \\(([(0-9]*)\\) FOR BIT DATA");
	static final Pattern VAR_BIT_DATA = Pattern.compile("VARCHAR \\(([(0-9]*)\\) FOR BIT DATA");






	AS400Text getText(int length, int ccsid) {
		if (ccsid != -1) {
			return new AS400Text(length, ccsid);
		}
		return new AS400Text(length);
	}

	AS400VarChar getVarText(int length, int bytesPerChar, Integer ccsid) {
		if (ccsid != -1) {
			return new AS400VarChar(length, bytesPerChar, ccsid);
		}
		return new AS400VarChar(length, bytesPerChar);
	}

	public AS400DataType toDataType(String schema, String table, String columnName, String type, int length,
			Integer precision) {
		switch (type) {
		case "DECIMAL":
			return new AS400PackedDecimal(length, precision);
		case "CHAR () FOR BIT DATA": // password fields - treat as binary
			return new AS400ByteArray(length);
		case "VARCHAR () FOR BIT DATA": // password fields - treat as binary
			return new AS400VarBin(length);
		case "CHAR":
			return getText(length, ccsidCache.getCcsid(schema, table, columnName));
		case "NCHAR":
			return getText(length * octetLengthCache.getBytesPerChar(schema, table, columnName),
					ccsidCache.getCcsid(schema, table, columnName));
		case "NVARCHAR":
			return getVarText(length, octetLengthCache.getBytesPerChar(schema, table, columnName),
					ccsidCache.getCcsid(schema, table, columnName));
		case "VGRAPH":
			return getVarText(length, octetLengthCache.getBytesPerChar(schema, table, columnName),
					ccsidCache.getCcsid(schema, table, columnName));
		case "TIMESTAMP":
			return AS400_TIMESTAMP;
		case "VARCHAR":
			return getVarText(length, octetLengthCache.getBytesPerChar(schema, table, columnName),
					ccsidCache.getCcsid(schema, table, columnName));
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
			final Optional<Integer> varLength = bitDataLengthFromRegex(type, length, VAR_BIT_DATA);
			if (varLength.isPresent()) {
				return new AS400VarBin(varLength.get());
			}
			final Optional<Integer> fixedLenght = bitDataLengthFromRegex(type, length, BIT_DATA);
			if (fixedLenght.isPresent()) {
				return new AS400ByteArray(fixedLenght.get());
			}
		}
		throw new IllegalArgumentException(String.format("Unsupported type %s for column %s", type, columnName));
	}

	private Optional<Integer> bitDataLengthFromRegex(String type, int length, Pattern regex) {
		final Matcher matcher = regex.matcher(type); // - treat as binary
		if (matcher.matches()) {
			final String size = matcher.group(1);
			if (size.isEmpty()) {
				return Optional.of(length);
			} else {
				final int l = Integer.parseInt(size);
				return Optional.of(l);
			}
		}
		return Optional.empty();
	}
}
