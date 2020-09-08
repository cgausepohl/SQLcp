/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/sqlcp
 */
package com.cg.sqlcp.impl;

public interface CLIParams {

	// Source Database Parameter, needed for db2db, db2file
	public static final String SRC_JDBC = "srcJDBC";
	public static final String SRC_USER = "srcUser";
	public static final String SRC_PASSWORD = "srcPassword";
	public static final String SRC_DATA = "srcData";
	public static final String SRC_BUFFERED_ROWS = "bufferedRows";

	// Target database parameter needed for db2db, file2db
	public static final String DEST_DB_JDBC = "destJDBC";
	public static final String DEST_DB_USER = "destUser";
	public static final String DEST_DB_PASSWORD = "destPassword";
	public static final String DEST_DB_TARGET = "destTarget";
	public static final String DEST_DB_BINDTYPES = "destBindTypes";
	public static final String DEST_DB_SQL_BEF_IMPORT = "destSQLBeforeImport";
	public static final String DEST_DB_NUM_THREADS = "destNumThreads";

	// general parameter
	public static final String BATCHSIZE = "batchSize";
	public static final String PRINTPARAMSONLY = "printParamsOnly";
	public static final String PRINTRUNTIMEINFO = "printRuntimeInfo";
	public static final String PRINTSUMMARY = "printSummary";
	public static final String GCINTERVALSEC = "GCIntervalSec";

	// output file used for db2file
	public static final String DEST_FILE_NAME = "destFile";
	public static final String DEST_FILE_INCLHEADER = "destInclHeader";
	public static final String DEST_FILE_FIELDSEPARATOR = "destSeparatorChar";
	public static final String DEST_FILE_DESTMODE = "destFileMode";
	public static final String DEST_FILE_COUNTROWS = "destFirstColIsCounter"; // YES
	public static final String DEST_FILE_FMT_NULL = "fmt.null";
	public static final String DEST_FILE_FMT_BOOLTRUE = "fmt:bool.true";
	public static final String DEST_FILE_FMT_BOOLFALSE = "fmt:bool.false";
	public static final String DEST_FILE_FMT_DATE = "fmt:date";
	public static final String DEST_FILE_FMT_TIME = "fmt:time";
	public static final String DEST_FILE_FMT_DATETIME = "fmt:datetime";
	public static final String DEST_FILE_FMT_TIMESTAMP = "fmt:timestamp";
	public static final String DEST_FILE_FMT_TIMESTAMPTZ = "fmt:timestamp_tz";
	public static final String DEST_FILE_FMT_CURRENCY = "fmt:currency";
	public static final String DEST_FILE_FMT_FLOAT = "fmt:float";

	public static final String[] HELPORDER_DB2DB = { SRC_JDBC, SRC_USER, SRC_PASSWORD, SRC_DATA, SRC_BUFFERED_ROWS, 
			"",
			DEST_DB_JDBC, DEST_DB_USER, DEST_DB_PASSWORD, DEST_DB_TARGET, DEST_DB_BINDTYPES, DEST_DB_SQL_BEF_IMPORT,
			DEST_DB_NUM_THREADS, 
			"", 
			BATCHSIZE, PRINTPARAMSONLY, PRINTRUNTIMEINFO, PRINTSUMMARY, GCINTERVALSEC };

	public static final String[] HELPORDER_DB2FILE = { SRC_JDBC, SRC_USER, SRC_PASSWORD, SRC_DATA, SRC_BUFFERED_ROWS,
			"", 
			PRINTPARAMSONLY, PRINTRUNTIMEINFO, PRINTSUMMARY, GCINTERVALSEC, 
			"", 
			DEST_FILE_NAME,	DEST_FILE_INCLHEADER, DEST_FILE_FIELDSEPARATOR, DEST_FILE_DESTMODE, DEST_FILE_COUNTROWS, 
			DEST_FILE_FMT_NULL,	DEST_FILE_FMT_FLOAT, DEST_FILE_FMT_BOOLTRUE, DEST_FILE_FMT_BOOLFALSE, 
			DEST_FILE_FMT_DATE,	DEST_FILE_FMT_TIME, DEST_FILE_FMT_DATETIME, DEST_FILE_FMT_TIMESTAMP, 
			DEST_FILE_FMT_TIMESTAMPTZ, DEST_FILE_FMT_CURRENCY };

}
