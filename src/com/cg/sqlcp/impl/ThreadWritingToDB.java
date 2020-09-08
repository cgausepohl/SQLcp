/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/sqlcp
 */
package com.cg.sqlcp.impl;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.StringTokenizer;

import com.cg.sqlutil.Row;
import com.cg.sqlutil.SQLUtilFactory;
import com.cg.sqlutil.SQLUtilInterface;

public class ThreadWritingToDB extends Thread {

	private SQLUtilInterface sql;
	private String target;
	private int[] bindTypes;
	private ThreadReadingFromDB in;
	private int rowsWritten = 0, batchesInserted = 0;
	private long msDBTime = 0L, msWaitTime = 0L, msInitTime = 0L;
	private Throwable exceptionDuringRun;
	private String jdbc, user, password;
	private String bindTypesParam;

	public ThreadWritingToDB(ThreadReadingFromDB in, String jdbc, String user, String password, String target,
			String bindTypes) throws SQLException {
		this.jdbc = jdbc;
		this.user = user;
		this.password = password;
		this.in = in;
		this.target = target;
		this.bindTypesParam = bindTypes;
	}

	private void init() throws SQLException {
		long t0 = System.currentTimeMillis();
		try {
			sql = SQLUtilFactory.createSQLUtil(jdbc, user, password);
		} catch (SQLException sqle) {
			System.err.println("Cannot establish connection to target. jdbc=" + jdbc + " user=" + user
					+ " len(password)=" + (password == null ? null : password.length()));
			throw sqle;
		}
		sql.getConnection().setReadOnly(false);
		sql.getConnection().setAutoCommit(false);
		msInitTime = System.currentTimeMillis() - t0;
		if (!Util.isNull(bindTypesParam)) {
			StringTokenizer st = new StringTokenizer(bindTypesParam, ",");
			ArrayList<Integer> l = new ArrayList<>();
			while (st.hasMoreTokens()) {
				String t = st.nextToken();
				switch (t) {
				case "ARRAY":
					l.add(Types.ARRAY);
					break;
				case "BIGINT":
					l.add(Types.BIGINT);
					break;
				case "BINARY":
					l.add(Types.BINARY);
					break;
				case "BIT":
					l.add(Types.BIT);
					break;
				case "BLOB":
					l.add(Types.BLOB);
					break;
				case "BOOLEAN":
					l.add(Types.BOOLEAN);
					break;
				case "CHAR":
					l.add(Types.CHAR);
					break;
				case "CLOB":
					l.add(Types.CLOB);
					break;
				case "DATALINK":
					l.add(Types.DATALINK);
					break;
				case "DATE":
					l.add(Types.DATE);
					break;
				case "DECIMAL":
					l.add(Types.DECIMAL);
					break;
				case "DISTINCT":
					l.add(Types.DISTINCT);
					break;
				case "DOUBLE":
					l.add(Types.DOUBLE);
					break;
				case "FLOAT":
					l.add(Types.FLOAT);
					break;
				case "INTEGER":
					l.add(Types.INTEGER);
					break;
				case "JAVA_OBJECT":
					l.add(Types.JAVA_OBJECT);
					break;
				case "LONGNVARCHAR":
					l.add(Types.LONGNVARCHAR);
					break;
				case "LONGVARBINARY":
					l.add(Types.LONGVARBINARY);
					break;
				case "LONGVARCHAR":
					l.add(Types.LONGVARCHAR);
					break;
				case "NCHAR":
					l.add(Types.NCHAR);
					break;
				case "NCLOB":
					l.add(Types.NCLOB);
					break;
				case "NULL":
					l.add(Types.NULL);
					break;
				case "NUMERIC":
					l.add(Types.NUMERIC);
					break;
				case "NVARCHAR":
					l.add(Types.NVARCHAR);
					break;
				case "OTHER":
					l.add(Types.OTHER);
					break;
				case "REAL":
					l.add(Types.REAL);
					break;
				case "REF":
					l.add(Types.REF);
					break;
				case "REF_CURSOR":
					l.add(Types.REF_CURSOR);
					break;
				case "ROWID":
					l.add(Types.ROWID);
					break;
				case "SMALLINT":
					l.add(Types.SMALLINT);
					break;
				case "SQLXML":
					l.add(Types.SQLXML);
					break;
				case "STRUCT":
					l.add(Types.STRUCT);
					break;
				case "TIME":
					l.add(Types.TIME);
					break;
				case "TIME_WITH_TIMEZONE":
					l.add(Types.TIME_WITH_TIMEZONE);
					break;
				case "TIMESTAMP":
					l.add(Types.TIMESTAMP);
					break;
				case "TIMESTAMP_WITH_TIMEZONE":
					l.add(Types.TIMESTAMP_WITH_TIMEZONE);
					break;
				case "TINYINT":
					l.add(Types.TINYINT);
					break;
				case "VARBINARY":
					l.add(Types.VARBINARY);
					break;
				case "VARCHAR":
					l.add(Types.VARCHAR);
					break;
				default:
					throw new IllegalArgumentException(
							"cannot map bindtype=" + t + ". Please see constants in java.sql.Types");
				}
			}
			this.bindTypes = new int[l.size()];
			for (int i = 0; i < this.bindTypes.length; i++)
				this.bindTypes[i] = l.get(i);
		}

	}

	public void executeSQLBeforeInserts(String ddl) throws SQLException {
		SQLUtilInterface sql = null;
		long t0 = System.currentTimeMillis();
		if (ddl != null && ddl.length() >= 2) {
			try {
				sql = SQLUtilFactory.createSQLUtil(jdbc, user, password);
				sql.executeDDL(ddl);
				sql.commit();
			} catch (SQLException sqle) {
				System.err.println("Cannot execute SQL on target. SQL=" + ddl);
				sqle.printStackTrace(System.err);
				throw sqle;
			} finally {
				if (sql != null)
					sql.closeConnection();
			}
		}
		msInitTime += System.currentTimeMillis() - t0;
	}

	private String createInsStmt(String target) throws SQLException {
		// if start with insert%(, then target is already the insert statement
		if (target.toUpperCase().startsWith("INSERT ")) {
			if (target.indexOf('(') > 0)
				return target;
		}
		String insSql = "insert into " + target + "(";
		for (int i = 1; i <= in.getColumnCount(); i++) {
			insSql += (i == 1 ? "" : ",") + in.getColumnName(i);
		}
		insSql += " ) values (?";
		for (int i = 2; i <= in.getColumnCount(); i++)
			insSql += ",?";
		insSql += ')';
		return insSql;
	}

	@Override
	public void run() {
		try {
			init();

			// from now on, target-table should be available
			String insertStmt = createInsStmt(target);
			while (true) {
				if (in.getState() == State.TERMINATED && in.getQueue().size() == 0)
					break;

				Row[] rows = in.getQueue().poll();
				if (rows == null) {
					long t0 = System.currentTimeMillis();
					Thread.sleep(50);
					msWaitTime += System.currentTimeMillis() - t0;
				} else {
					long t0 = System.currentTimeMillis();
					/* int[] dmlCodes = */sql.executeDMLBatch(insertStmt, rows,
							this.bindTypes != null ? this.bindTypes : in.getColumnTypes());
					sql.commitSilent();
					batchesInserted++;
					// todo: check dmlCodes for error, print data of rows[idx_of_dmlCodes[hasError]]
					rowsWritten += rows.length;
					long dur = System.currentTimeMillis() - t0;
					msDBTime += dur;
					// try to free ressources
					for (int i = 0; i < rows.length; i++)
						rows[i] = null;
					rows = null;
				}
			}
			sql.commitSilent();
		} catch (Throwable t) {
			exceptionDuringRun = t;
			throw new RuntimeException(t);
		} finally {
			cleanUpAfterRun();
		}
	}

	public Throwable getException() {
		return exceptionDuringRun;
	}

	public void cleanUpAfterRun() {
		if (sql != null)
			sql.closeConnection();
	}

	public synchronized long getDBTime() {
		return msDBTime;
	}

	public synchronized long getInitTime() {
		return msInitTime;
	}

	public synchronized long getWaitForQueueProducer() {
		return msWaitTime;
	}

	public synchronized int getTotalRowsInserted() {
		return rowsWritten;
	}

	public int getTotalBatchedInserts() {
		return batchesInserted;
	}

}
