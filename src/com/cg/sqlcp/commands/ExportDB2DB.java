/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/sqlcp
 */
package com.cg.sqlcp.commands;

import java.io.IOException;
import java.lang.Thread.State;
import java.sql.SQLException;
import java.util.concurrent.LinkedTransferQueue;

import com.cg.cli.CLIParsed;
import com.cg.cli.CLIParsedException;
import com.cg.cli.CLIRules;
import com.cg.sqlcp.SQLcp;
import com.cg.sqlcp.impl.CLIParams;
import com.cg.sqlcp.impl.ThreadReadingFromDB;
import com.cg.sqlcp.impl.ThreadWritingToDB;
import com.cg.sqlcp.impl.Util;
import com.cg.sqlutil.Row;

public class ExportDB2DB {

	private Long tGCwait;
	private long tInsertAll = 0, t0Start = System.currentTimeMillis();
	private Long memPeakM = 0L;
	private CLIParsed cliParsed;

	public ExportDB2DB() {
	}

	public void start(String[] args) throws SQLException, IOException, InterruptedException {
		CLIRules rules = new CLIRules(
				"copies the result of a SELECT directly via INSERT: " + SQLcp.class.getName() + " db2db ");
		rules.addRequired(CLIParams.SRC_JDBC).setDescription("Source: JDBC Connection String");
		rules.addRequired(CLIParams.SRC_USER).setDescription("Source: Username");
		rules.addRequired(CLIParams.SRC_PASSWORD).setDescription("Source: Password");
		rules.addRequired(CLIParams.SRC_DATA).setDescription("Source: Tablename or Select Query");
		rules.addOptional(CLIParams.SRC_BUFFERED_ROWS, "50000")
				.setDescription("Source: Maximum number of rows queued to be written to Target");

		rules.addRequired(CLIParams.DEST_DB_JDBC).setDescription("Target: JDBC Connection String");
		rules.addRequired(CLIParams.DEST_DB_USER).setDescription("Target: Username");
		rules.addRequired(CLIParams.DEST_DB_PASSWORD).setDescription("Target: Password");
		rules.addRequired(CLIParams.DEST_DB_TARGET)
				.setDescription("Target: Tablename where the data will be written into");
		rules.addOptional(CLIParams.DEST_DB_SQL_BEF_IMPORT, "")
				.setDescription("Target: ???create table, truncate, delete of target object");
		rules.addOptional(CLIParams.DEST_DB_NUM_THREADS, "1").setDescription("Target: number of writing threads");
		rules.addOptional(CLIParams.DEST_DB_BINDTYPES, "").setDescription("Target: ???");

		rules.addOptional(CLIParams.BATCHSIZE, "5000")
				.setDescription("number of rows that are read or written per chunk");
		rules.addOptional(CLIParams.GCINTERVALSEC, "0")
				.setDescription("Call the Java Memory Garbage Collector every n Seconds, 0=JVM Managed");

		rules.addFlag(CLIParams.PRINTPARAMSONLY, "YES=Print given parameters only, then exit");
		rules.addOptional(CLIParams.PRINTRUNTIMEINFO, "0")
				.setDescription("Interval of seconds when runtime info will be printed, 0=no stats during execution");
		rules.addFlag(CLIParams.PRINTSUMMARY, "Print statistics and used settings");

		try {
			cliParsed = new CLIParsed(rules, args);
		} catch (CLIParsedException pe) {
			System.err.println(pe.getMessage());
			rules.printHelp(CLIParams.HELPORDER_DB2DB);
			System.exit(1);
		}

		if (cliParsed.hasFlag(CLIParams.PRINTPARAMSONLY)) {
			cliParsed.printParams(System.out);
			return;
		}

		// start real work here
		ThreadWritingToDB outInit = null;
		ThreadReadingFromDB in = null;
		ThreadWritingToDB[] outThreads = null;

		LinkedTransferQueue<Row[]> queue = new LinkedTransferQueue<>();
		try {
			// init and start reading thread
			int batchSize = cliParsed.getIntegerArgument(CLIParams.BATCHSIZE);
			in = new ThreadReadingFromDB(cliParsed.getArgument(CLIParams.SRC_JDBC),
					cliParsed.getArgument(CLIParams.SRC_USER), cliParsed.getArgument(CLIParams.SRC_PASSWORD),
					cliParsed.getArgument(CLIParams.SRC_DATA),
					cliParsed.getIntegerArgument(CLIParams.SRC_BUFFERED_ROWS), batchSize, queue);
			in.start();

			// writing thread
			String destJdbc = cliParsed.getArgument(CLIParams.DEST_DB_JDBC);
			String destUser = cliParsed.getArgument(CLIParams.DEST_DB_USER);
			String destPassword = cliParsed.getArgument(CLIParams.DEST_DB_PASSWORD);
			String destTarget = cliParsed.getArgument(CLIParams.DEST_DB_TARGET);
			String destBindTypes = cliParsed.getArgument(CLIParams.DEST_DB_BINDTYPES);
			String sqlBeforeImport = cliParsed.getArgument(CLIParams.DEST_DB_SQL_BEF_IMPORT);
			// init first writing thread and execute sqlBeforeWrite if given
			outInit = new ThreadWritingToDB(in, destJdbc, destUser, destPassword, destTarget, destBindTypes);
			outInit.executeSQLBeforeInserts(sqlBeforeImport);

			// init and start all threads
			tInsertAll = System.currentTimeMillis();
			int destNumThreads = cliParsed.getIntegerArgument(CLIParams.DEST_DB_NUM_THREADS);
			outThreads = new ThreadWritingToDB[destNumThreads];
			for (int i = 0; i < destNumThreads; i++) {
				if (i == 0)
					outThreads[i] = outInit;
				else
					outThreads[i] = new ThreadWritingToDB(in, destJdbc, destUser, destPassword, destTarget,
							destBindTypes);
				outThreads[i].start();
			}

			// print status or sleep while read/write threads active
			int gcIntervalSec = cliParsed.getIntegerArgument(CLIParams.GCINTERVALSEC);
			long lastGC = System.currentTimeMillis();
			long lastPrintRuntime = 0;
			while (atLeastOneNotTerminated(outThreads)) {
				// runtime-info
				int runtimeInfoInterval = cliParsed.getIntegerArgument(CLIParams.PRINTRUNTIMEINFO);
				if (runtimeInfoInterval > 0) {
					long currT = System.currentTimeMillis();
					if ((currT - lastPrintRuntime) / 1000 >= runtimeInfoInterval) {
						printStatus(in, outThreads, queue);
						lastPrintRuntime = System.currentTimeMillis();
					}
				}

				// manual gc mngmt
				if (gcIntervalSec > 0) {
					if (lastGC + (gcIntervalSec * 1000) < System.currentTimeMillis()) {
						long t0 = System.currentTimeMillis();
						System.gc();
						if (tGCwait == null)
							tGCwait = 0L;
						tGCwait += System.currentTimeMillis() - t0;
						lastGC = System.currentTimeMillis();
					}
				}

				Thread.sleep(100);
			}

			// summary and done
			tInsertAll = System.currentTimeMillis() - tInsertAll;
			if (cliParsed.hasFlag(CLIParams.PRINTSUMMARY))
				printSummary(in, outThreads);

			// check for errors
			int errCnt = 0;
			for (ThreadWritingToDB t : outThreads)
				if (t.getException() != null) {
					errCnt++;
					t.getException().printStackTrace(System.err);
				}

			if (errCnt == 0)
				Util.log("copy done");
			else {
				Util.log("copy failed");
				System.exit(1);
			}
		} catch (Throwable t) {
			t.printStackTrace();
			System.exit(1);
		} finally {
			if (in != null) {
				in.terminate();
				in.cleanUpAfterRun();
			}
			if (outThreads != null)
				for (ThreadWritingToDB out : outThreads)
					out.cleanUpAfterRun();
		}
	}

	private boolean atLeastOneNotTerminated(ThreadWritingToDB[] outThreads) {
		for (ThreadWritingToDB out : outThreads)
			if (out.getException() == null && out.getState() != State.TERMINATED)
				return true;
		return false;
	}

	private void printStatus(ThreadReadingFromDB in, ThreadWritingToDB[] outThreads, LinkedTransferQueue<Row[]> queue) {
		StringBuffer msg = new StringBuffer();
		Long memUsgM = Long.valueOf(Runtime.getRuntime().totalMemory() / 1024 / 1024);
		if (memUsgM > memPeakM)
			memPeakM = memUsgM;
		msg.append("mem=").append(memUsgM.toString()).append("M; ");
		msg.append("queue=" + Util.getRowCountOfQueue(queue) + "; ");
		msg.append("T=" + formatMs((System.currentTimeMillis() - t0Start)) + "; ");
		msg.append("in(" + in.getState() + " rcvd=" + in.getTotalRowsReceived() + " dbT=" + formatMs(in.getDBTime())
				+ "; waitT=" + formatMs(in.getWaitForQueueConsumer()) + "); ");
		msg.append("out*" + outThreads.length + "(" + getStates(outThreads) + " ins="
				+ getSumTotalRowsInserted(outThreads) + " dbT=" + formatMs(getSumDBTime(outThreads)) + "; waitT="
				+ formatMs(getSumWaitForQueueProducer(outThreads)) + ")");
		Util.log(msg.toString());
	}

	private void printSummary(ThreadReadingFromDB in, ThreadWritingToDB[] outThreads) {

		Util.log("SUMMARY");

		// source : host=?, user=?, data=[[?]]
		Util.log("source     : host=" + cliParsed.getArgument(CLIParams.SRC_JDBC) + ", user="
				+ cliParsed.getArgument(CLIParams.SRC_USER) + ", data=[[" + cliParsed.getArgument(CLIParams.SRC_DATA)
				+ "]]");
		// destination: host=?, user=?, data=[[?]]
		Util.log("destination: host=" + cliParsed.getArgument(CLIParams.DEST_DB_JDBC) + ", user="
				+ cliParsed.getArgument(CLIParams.DEST_DB_USER) + ", target="
				+ cliParsed.getArgument(CLIParams.DEST_DB_TARGET));

		// readProc : init=6611ms, wait=8167ms, fetch=121ms, 2831rows/sec, 19059rows
		// fetched
		Util.log("readProc   : init=" + formatMs(in.getInitTime()) + ", wait=" + formatMs(in.getWaitForQueueConsumer())
				+ ", " + "fetch=" + formatMs(in.getDBTime()) + ", "
				+ getRowsPerSec(in.getTotalRowsReceived(), in.getDBTime() + in.getInitTime()) + "rows/sec, "
				+ in.getTotalRowsReceived() + "rows fetched");
		// writeProc : init=14695ms, wait=107ms, threads=8, insert=53936ms, 167rows/sec,
		// 96*ps.executeBatch()/commit, 19059rows inserted
		long getSumDBTime_ = getSumDBTime(outThreads);
		Util.log("writeProc  : init=" + formatMs(getSumInitTime(outThreads)) + ", wait="
				+ formatMs(getSumWaitForQueueProducer(outThreads)) + ", " + "threads=" + outThreads.length + ", insert="
				+ formatMs(getSumDBTime(outThreads)) + ", " + getRowsPerSec(tInsertAll, getSumDBTime_) + "rows/sec, "
				+ getSumTotalBatchedInserts(outThreads) + "*ps.executeBatch()/commit, "
				+ getSumTotalRowsInserted(outThreads) + "rows inserted");
		// summary : execTime=18839ms, rows=?, (rows/sec)=?memPeak=53M, outThreads=8,
		// rows=20447, (rows/sec)=1135
		long overallMs = System.currentTimeMillis() - t0Start;
		StringBuffer sb = new StringBuffer(100);
		sb.append("summary    : execTime=" + formatMs(overallMs));
		if (tGCwait != null)
			sb.append(", gcTime=" + formatMs(tGCwait));
		sb.append(", memPeak=" + memPeakM + "M");
		sb.append(", outThreads=" + outThreads.length);
		sb.append(", rows=" + in.getTotalRowsReceived());
		sb.append(", (rows/sec)=" + getRowsPerSec(in.getTotalRowsReceived(), overallMs));
		Util.log(sb.toString());
	}

	private String formatMs(long ms) {
		// stay ms under 10sec
		if (ms < 10000)
			return "" + ms + "ms";
		// switch to sec
		long s = ms / 1000;
		if (s < 3600)
			return "" + s + "sec";
		long mi = s / 60;
		return "" + mi + "m";
	}

	private long getRowsPerSec(long tInsertAll, long getSumDBTime_) {
		if (getSumDBTime_ / 1000 == 0)
			return -1;
		return (long) ((double) tInsertAll / (getSumDBTime_ / 1000));
	}

	private long getSumDBTime(ThreadWritingToDB[] outThreads) {
		long sum = 0;
		if (outThreads != null)
			for (ThreadWritingToDB out : outThreads)
				sum += out.getDBTime();
		return sum;
	}

	private long getSumTotalRowsInserted(ThreadWritingToDB[] outThreads) {
		long sum = 0;
		if (outThreads != null)
			for (ThreadWritingToDB out : outThreads)
				sum += out.getTotalRowsInserted();
		return sum;
	}

	private long getSumWaitForQueueProducer(ThreadWritingToDB[] outThreads) {
		long sum = 0;
		if (outThreads != null)
			for (ThreadWritingToDB out : outThreads)
				sum += out.getWaitForQueueProducer();
		return sum;
	}

	private long getSumInitTime(ThreadWritingToDB[] outThreads) {
		long sum = 0;
		if (outThreads != null)
			for (ThreadWritingToDB out : outThreads)
				sum += out.getInitTime();
		return sum;
	}

	private long getSumTotalBatchedInserts(ThreadWritingToDB[] outThreads) {
		long sum = 0;
		if (outThreads != null)
			for (ThreadWritingToDB out : outThreads)
				sum += out.getTotalBatchedInserts();
		return sum;
	}

	private void hlpGetStateStat(ThreadWritingToDB[] outThreads, State s, StringBuffer currSb) {
		int cnt = 0;
		if (outThreads != null) {
			for (ThreadWritingToDB t : outThreads) {
				if (t != null) {
					if (s.equals(t.getState()))
						cnt++;
				}
			}
		}
		if (cnt > 0) {
			if (currSb.length() > 0)
				currSb.append(',');
			currSb.append(s.toString()).append('*').append(cnt);
		}
	}

	private String getStates(ThreadWritingToDB[] outThreads) {
		StringBuffer sb = new StringBuffer(100);
		// could be done in a loop, but i want a guaranteed order of states in the
		// output string
		hlpGetStateStat(outThreads, State.NEW, sb);
		hlpGetStateStat(outThreads, State.RUNNABLE, sb);
		hlpGetStateStat(outThreads, State.BLOCKED, sb);
		hlpGetStateStat(outThreads, State.WAITING, sb);
		hlpGetStateStat(outThreads, State.TIMED_WAITING, sb);
		hlpGetStateStat(outThreads, State.TERMINATED, sb);
		return sb.toString();
	}

}
