/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/sqlcp
 */
package com.cg.sqlcp.impl;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.LinkedTransferQueue;

import com.cg.sqlutil.Row;
import com.cg.sqlutil.SQLUtilFactory;
import com.cg.sqlutil.SQLUtilInterface;

public class ThreadReadingFromDB extends Thread {

	private SQLUtilInterface sql = null;
    private LinkedTransferQueue<Row[]> queue;
    private int maxQueueSize;
    private String selectStmt;
    private int rowsTotalRead = 0;
    private int fetchesExecuted = 0;
    private long tDBTime=0;
	private long tWaitForQueueConsumer=0;
	private boolean isTerminated = false;
	private long tInitTime = 0;

	public ThreadReadingFromDB(String jdbc, String user, String password, String queryData, int maxQueueSize, int batchSize,
			LinkedTransferQueue<Row[]> queue) throws SQLException, IOException {
		this.queue = queue;
		this.maxQueueSize = maxQueueSize;
		try {
			long t0 = System.currentTimeMillis();
			try {
				sql = SQLUtilFactory.createSQLUtil(jdbc, user, password);
			} catch (SQLException sqle) {
				System.err.println("Cannot establish connection to source. jdbc="+jdbc+" user="+user+" len(password)="+(password==null?0:password.length()));
				throw sqle;
			}
			sql.getConnection().setReadOnly(true);
			sql.setFetchSize(batchSize);
			// if contains "select " then its a statement(select, with...)
			String srcType = queryData.toLowerCase().replaceFirst(".*select\\s.*", "SQL");
			if ("SQL".equals(srcType))
				selectStmt = queryData;
			else
				selectStmt = "SELECT * FROM "+queryData;

        	// First chunk: get metadata
        	sql.getChunksPrepare(selectStmt, batchSize);
        	tInitTime = System.currentTimeMillis() - t0;
		} catch (SQLException e) {
			cleanUpAfterRun();
			throw e;
		}
	}

	public long getInitTime() {
		return tInitTime;
	}

	public void cleanUpAfterRun() {
		if (sql != null)
			sql.closeConnection();
	}

	public synchronized void terminate() {
    	isTerminated = true;
	}

	@Override
    public void run() {
		long t0;
        try {
    		Row[] rows = null;
        	while (true) {
        		t0 = System.currentTimeMillis();
        		rows=sql.getChunksGetNextRows();
        		tDBTime += System.currentTimeMillis() - t0;
        		if (rows==null) break;
        		fetchesExecuted++;
        		rowsTotalRead += rows.length;
            	if (isTerminated) return;
        		queue.add(rows);
                while (Util.getRowCountOfQueue(queue) >= maxQueueSize) {
                	if (isTerminated) return;
                	t0 = System.currentTimeMillis();
                    Thread.sleep(50);
                    tWaitForQueueConsumer += System.currentTimeMillis()-t0;
                }
        	}
            while (queue.size() > 0) {
            	if (isTerminated) return;
            	t0 = System.currentTimeMillis();
                Thread.sleep(50);
                tWaitForQueueConsumer += System.currentTimeMillis()-t0;
            }
        } catch (Throwable t) {
			throw new RuntimeException(t);
        } finally {
        	try {
        		sql.getChunksClose();
        	} finally {
        		cleanUpAfterRun();
        	}
        }
    }

	public Integer getMaxQueueSize() {
		return maxQueueSize;
	}

	public synchronized int getFetchesExecuted() {
		return fetchesExecuted;
	}

	public int[] getColumnTypes() {
		return sql.getPreviousRowSQLTypes();
	}

	public synchronized int getTotalRowsReceived() {
		return rowsTotalRead;
	}

	public synchronized int getColumnCount() throws SQLException {
		return sql.getPreviousMetaData().getColumnCount();
	}
	
	public synchronized String getColumnName(int idx) throws SQLException {
		return sql.getPreviousMetaData().getColumnName(idx);
	}

	public LinkedTransferQueue<Row[]> getQueue() {
		return queue;
	}

	public synchronized long getDBTime() {
		return tDBTime;
	}

	public synchronized long getWaitForQueueConsumer() {
		return tWaitForQueueConsumer;
	}

}