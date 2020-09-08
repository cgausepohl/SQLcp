/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/sqlcp
 */
package com.cg.sqlcp;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Map;

public class RuntimeInfo {

	// private static final Logger log = Logger.getLogger(SQLcp.class.getName());
	private static String writePrefix = "";

	public static void setWritePrefix(String prefix) {
		RuntimeInfo.writePrefix = prefix;
	}

	// todo change to file output
	private static void writeln(String s) {
		System.out.println(writePrefix + s);
	}

	public static void printPublicMembers(Object obj, String ignore) {
		Class<?> objClass = obj.getClass();

		Field[] fields = objClass.getFields();
		for (Field field : fields) {
			String fn = field.getName();
			if (fn.equals(ignore))
				writeln(fn + ":*****");
			else {
				try {
					writeln(fn + ':' + field.get(obj));
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		}
	}

	public static void printResultSet(ResultSet rs) throws SQLException {
		// header
		StringBuilder sb = new StringBuilder("#: ");
		int maxColCnt = rs.getMetaData().getColumnCount();
		for (int i = 1; i <= maxColCnt; i++) {
			sb.append(rs.getMetaData().getColumnLabel(i));
			if (i < maxColCnt)
				sb.append(';');
		}
		writeln(sb.toString());
		// data
		int i = 1;
		while (rs.next()) {
			sb.setLength(0);
			sb.append('#').append(i++).append(": ");
			for (int n = 1; n <= maxColCnt; n++) {
				String type = rs.getMetaData().getColumnTypeName(n);
				if ("VARCHAR".equals(type) || "VARCHAR2".equals(type) || "CHAR".equals(type) || "TEXT".equals(type)
						|| "CLOB".equals(type) || "NCHAR".equals(type) || "NVARCHAR".equals(type)
						|| "NVARCHAR2".equals(type)) {
					sb.append('"').append(rs.getString(n)).append('"');
				} else {
					sb.append(rs.getString(n));
				}
				if (n < maxColCnt)
					sb.append(";");
			}
			writeln(sb.toString());
		}
	}

	public static void printJDBCInfo(Connection con) throws SQLException {
		ResultSet rs = null;
		try {
			setWritePrefix("Connection.");
			writeln("con=" + con);
			writeln("getAutoCommit():" + con.getAutoCommit());
			writeln("getTransactionIsolation():" + con.getTransactionIsolation());
			writeln("isValid(2):" + con.isValid(2));
			SQLWarning w = con.getWarnings();
			writeln("getWarnings():" + (w == null ? null : w.toString()));
			setWritePrefix(con.getClass().getName() + ".");
			printPublicMembers(con, null);
			writeln("getTypeMap():");
			Map<String, Class<?>> map = con.getTypeMap();
			if (map == null) {
				writeln("(returned map is null)");
			} else {
				for (String s : map.keySet()) {
					writeln(s + "=>" + map.get(s));
				}
			}
			setWritePrefix("DatabaseMetaData.");
			DatabaseMetaData dbMetaData = con.getMetaData();

			writeln("getDriverName():" + dbMetaData.getDriverName());
			writeln("getDriverVersion():" + dbMetaData.getDriverVersion());
			writeln("getDriverMajorVersion():" + dbMetaData.getDriverMajorVersion());
			writeln("getDriverMinorVersion():" + dbMetaData.getDriverMinorVersion());
			writeln("getDriverVersion():" + dbMetaData.getDriverVersion());

			writeln("getJDBCMajorVersion():" + dbMetaData.getJDBCMajorVersion());
			writeln("getJDBCMinorVersion():" + dbMetaData.getJDBCMinorVersion());

			writeln("getDatabaseProductName():" + dbMetaData.getDatabaseProductName());
			writeln("getDatabaseProductVersion():" + dbMetaData.getDatabaseProductVersion());
			writeln("getDatabaseMajorVersion():" + dbMetaData.getDatabaseMajorVersion());
			writeln("getDatabaseMinorVersion():" + dbMetaData.getDatabaseMinorVersion());

			writeln("getSystemFunctions():" + dbMetaData.getSystemFunctions());
			writeln("getSystemFunctions():" + dbMetaData.getTimeDateFunctions());
			writeln("getNumericFunctions():" + dbMetaData.getNumericFunctions());
			writeln("getStringFunctions():" + dbMetaData.getStringFunctions());
			writeln("getSQLKeywords():" + dbMetaData.getSQLKeywords());
			writeln("getSQLStateType():" + dbMetaData.getSQLStateType());

			writeln("allTablesAreSelectable():" + dbMetaData.allTablesAreSelectable());
			writeln("autoCommitFailureClosesAllResultSets():" + dbMetaData.autoCommitFailureClosesAllResultSets());
			writeln("doesMaxRowSizeIncludeBlobs():" + dbMetaData.doesMaxRowSizeIncludeBlobs());
			writeln("getMaxRowSize():" + dbMetaData.getMaxRowSize());
			writeln("getMaxStatementLength():" + dbMetaData.getMaxStatementLength());
			writeln("getMaxTablesInSelect():" + dbMetaData.getMaxTablesInSelect());
			writeln("supportsGetGeneratedKeys():" + dbMetaData.supportsGetGeneratedKeys());

			writeln("getExtraNameCharacters():" + dbMetaData.getExtraNameCharacters());
			writeln("getIdentifierQuoteString():" + dbMetaData.getIdentifierQuoteString());
			writeln("getSearchStringEscape():" + dbMetaData.getSearchStringEscape());

			rs = dbMetaData.getTypeInfo();
			setWritePrefix("");
			writeln("Connection.DatabaseMetaData.getTypeInfo():" + rs);
			setWritePrefix("Connection.DatabaseMetaData.getTypeInfo():");
			printResultSet(rs);

			rs = dbMetaData.getClientInfoProperties();
			setWritePrefix("");
			writeln("DatabaseMetaData.getClientInfoProperties():" + rs);
			setWritePrefix("DatabaseMetaData.getClientInfoProperties():");
			printResultSet(rs);
		} finally {
			rs.close();
		}
	}
}