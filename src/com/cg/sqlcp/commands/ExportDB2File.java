/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/sqlcp
 */
package com.cg.sqlcp.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.LinkedTransferQueue;

import com.cg.cli.CLIParsed;
import com.cg.cli.CLIParsedException;
import com.cg.cli.CLIRules;
import com.cg.sqlcp.SQLcp;
import com.cg.sqlcp.impl.CLIParams;
import com.cg.sqlcp.impl.ThreadReadingFromDB;
import com.cg.sqlcp.impl.Util;
import com.cg.sqlutil.Row;

public class ExportDB2File {

	private long tStarted = -1, tFinished = -1, tFileWriteTime = 0;
	private long rowsExported = 0;
	private long charsWritten = 0;
	private long maxMemUsage = -1;
	private boolean firstColIsRowCounter = false, writeColnamesAsFirstRow = false, firstRowWritten = false;
	private BufferedWriter w = null;

	public void start(String[] args) {
		tStarted = System.currentTimeMillis();

		CLIRules rules = new CLIRules(
				"copies the result of a SELECT to a file: " + SQLcp.class.getName() + " db2file ");
		rules.addRequired(CLIParams.SRC_JDBC).setDescription("Source: JDBC Connection String");
		rules.addRequired(CLIParams.SRC_USER).setDescription("Source: Username");
		rules.addRequired(CLIParams.SRC_PASSWORD).setDescription("Source: Password");
		rules.addRequired(CLIParams.SRC_DATA).setDescription("Source: Tablename or Select Query");

		rules.addOptional(CLIParams.BATCHSIZE, "5000")
				.setDescription("number of rows that are read or written per chunk");
		rules.addOptional(CLIParams.SRC_BUFFERED_ROWS, "50000")
				.setDescription("Source: Maximum number of rows queued to be written to Target");

		rules.addOptional(CLIParams.DEST_FILE_NAME, null)
				.setDescription("Target: filename, if not given console will become target");
		// +++rules.addOptional(CLIParams.DEST_FILE_INCLHEADER,
		// "NO").setDescription("YES=first line of destFile will contain columnnames");
		rules.addFlag(CLIParams.DEST_FILE_INCLHEADER)
				.setDescription("YES=first line of destFile will contain columnnames");
		rules.addOptional(CLIParams.DEST_FILE_FIELDSEPARATOR, ";")
				.setDescription("character to separate different values within the same line");
		rules.addOptional(CLIParams.DEST_FILE_DESTMODE, "OVERWRITE").setDescription("filemode for targetfile")
				.setValidValues("OVERWRITE", "APPEND");
		// +++
		rules.addFlag(CLIParams.DEST_FILE_COUNTROWS).setDescription("first column becomes row number counter");
		rules.addOptional(CLIParams.DEST_FILE_FMT_NULL, "").setDescription("output value for NULL values");
		rules.addOptional(CLIParams.DEST_FILE_FMT_BOOLTRUE, "TRUE").setDescription("output value for SQL BOOLEAN:TRUE");
		rules.addOptional(CLIParams.DEST_FILE_FMT_BOOLFALSE, "FALSE")
				.setDescription("output value for SQL BOOLEAN:FALSE");
		rules.addOptional(CLIParams.DEST_FILE_FMT_DATE, "+++")
				.setDescription("output value for SQL DATE. see JAVA SimpleDateFormat");
		rules.addOptional(CLIParams.DEST_FILE_FMT_TIME, "+++")
				.setDescription("output value for SQL TIME. see JAVA SimpleDateFormat");
		rules.addOptional(CLIParams.DEST_FILE_FMT_DATETIME, "+++")
				.setDescription("foutput value for SQL DATETIME. see JAVA SimpleDateFormat");
		rules.addOptional(CLIParams.DEST_FILE_FMT_TIMESTAMP, "+++")
				.setDescription("output value for SQL TIMESTAMP. see JAVA SimpleDateFormat");
		rules.addOptional(CLIParams.DEST_FILE_FMT_TIMESTAMPTZ, "+++")
				.setDescription("output value for SQL TIMESTAMP WITH TIMEZONE. see JAVA SimpleDateFormat");
		rules.addOptional(CLIParams.DEST_FILE_FMT_CURRENCY, "+++")
				.setDescription("output value for SQL CURRENCY. see JAVA SimpleDateFormat");
		rules.addOptional(CLIParams.DEST_FILE_FMT_FLOAT, "+++")
				.setDescription("output value for SQL NUMERIC(and subtypes). see JAVA DecimalFormat");

		// +++
		rules.addFlag(CLIParams.PRINTPARAMSONLY).setDescription("Print given parameters only, then exit");
		// +++
		rules.addFlag(CLIParams.PRINTSUMMARY).setDescription("Print statistics and used settings");

		CLIParsed parsed = null;
		try {
			parsed = new CLIParsed(rules, args);
		} catch (CLIParsedException pe) {
			System.err.println(pe.getMessage());
			rules.printHelp(CLIParams.HELPORDER_DB2FILE);
			pe.printStackTrace();
			System.exit(1);
		}

		if (parsed.hasFlag(CLIParams.PRINTPARAMSONLY)) {
			parsed.printParams(System.out);
			return;
		}

		firstColIsRowCounter = parsed.hasFlag(CLIParams.DEST_FILE_COUNTROWS);
		writeColnamesAsFirstRow = parsed.hasFlag(CLIParams.DEST_FILE_INCLHEADER);

		boolean destAppend = "APPEND".equals(parsed.getArgument(CLIParams.DEST_FILE_DESTMODE));
		boolean destOverwrite = "OVERWRITE".equals(parsed.getArgument(CLIParams.DEST_FILE_DESTMODE));

		ThreadReadingFromDB in = null;
		LinkedTransferQueue<Row[]> queue = new LinkedTransferQueue<>();
		try {
			// init and start reading thread
			int batchSize = parsed.getIntegerArgument(CLIParams.BATCHSIZE);
			in = new ThreadReadingFromDB(parsed.getArgument(CLIParams.SRC_JDBC), parsed.getArgument(CLIParams.SRC_USER),
					parsed.getArgument(CLIParams.SRC_PASSWORD), parsed.getArgument(CLIParams.SRC_DATA),
					parsed.getIntegerArgument(CLIParams.SRC_BUFFERED_ROWS), batchSize, queue);
			in.start();

			if (parsed.getArgument(CLIParams.DEST_FILE_NAME) != null)
				w = prepareDestFile(destOverwrite, destAppend, parsed.getArgument(CLIParams.DEST_FILE_NAME));
			boolean firstLineDone = true;
			if (parsed.hasFlag(CLIParams.DEST_FILE_INCLHEADER))
				firstLineDone = false;
			String separator = parsed.getArgument(CLIParams.DEST_FILE_FIELDSEPARATOR);
			while (in.isAlive()) {
				if (in.getQueue().size() == 0) {
					Thread.sleep(20);
					continue;
				}
				Row[] rows = in.getQueue().poll();
				if (!firstLineDone) {
					// TODO: do header line
					firstLineDone = true;
				}

				writeRows(rows, in, w, separator);

				// get mem peak
				long currMax = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
				if (currMax > maxMemUsage)
					maxMemUsage = currMax;
			}
			long t0 = System.currentTimeMillis();
			if (w != null) {
				w.flush();
				w.close();
			}
			tFileWriteTime += System.currentTimeMillis() - t0;

			tFinished = System.currentTimeMillis();

			if (parsed.hasFlag(CLIParams.PRINTSUMMARY))
				printSummary(in, parsed);

		} catch (Throwable t) {
			t.printStackTrace();
			System.exit(1);
		} finally {
			closeFile(w);
			closeInThread(in);
		}
	}

	private void printSummary(ThreadReadingFromDB in, CLIParsed cli) {
		if (cli.getArgument(CLIParams.DEST_FILE_NAME) != null) {
			Util.log("target=" + cli.getArgument(CLIParams.DEST_FILE_NAME));
			Util.log("mode=" + cli.getArgument(CLIParams.DEST_FILE_DESTMODE));
		} else {
			Util.log("target=Console");
		}
		Util.log("started=" + new Date(tStarted));
		Util.log("finished=" + new Date(tFinished));
		long durMs = (tFinished - tStarted);
		Util.log("time execution complete(ms)=" + durMs);
		Util.log("rows exported=" + rowsExported);
		Util.log("time connect to source database (ms)=" + in.getInitTime());
		Util.log("time read from source database (ms)=" + in.getDBTime());
		Util.log("time wait, db thread in-buffer full (max=" + in.getMaxQueueSize()
				+ "), waiting for writing thread(ms)=" + in.getWaitForQueueConsumer());
		Util.log("time output(ms)=" + tFileWriteTime);
		long outSize = charsWritten;
		if (cli.getArgument(CLIParams.DEST_FILE_NAME) != null)
			outSize = new File(cli.getArgument(CLIParams.DEST_FILE_NAME)).length();
		Util.log("output size(bytes)=" + outSize);
		long bytesPerSec = (long) (outSize / ((double) durMs / 1000));
		Util.log("bytes/sec exported=" + bytesPerSec);
		Util.log("mb/sec exported=" + ((double) bytesPerSec / (1024 * 1024)));
		Util.log("rows/sec exported=" + (long) (rowsExported / ((double) durMs / 1000)));
		Util.log("max memory usage (mb)=" + Long.valueOf(maxMemUsage / (1024 * 1024)));
	}

	private BufferedWriter prepareDestFile(boolean destOverwrite, boolean destAppend, String destfile)
			throws IOException {
		File f = new File(destfile);
		if (f.exists()) {
			if (f.isDirectory()) {
				throw new FileAlreadyExistsException("destination file is a directory:" + f.getAbsoluteFile());
			} else if (!destOverwrite) {
				throw new FileAlreadyExistsException("destination file already exists:" + f.getAbsoluteFile());
			} else {
				f.delete();
			}
		}
		return new BufferedWriter(new FileWriter(destfile, destAppend));
	}

	private void closeInThread(ThreadReadingFromDB in) {
		try {
			if (in != null) {
				in.terminate();
				in.cleanUpAfterRun();
			}
		} catch (Exception e) {
			Util.log(e);
		}

	}

	private void closeFile(BufferedWriter w) {
		try {
			if (w != null)
				w.close();
		} catch (IOException ioe) {
			Util.log(ioe);
		}

	}

	private void writeRows(final Row[] rows, final ThreadReadingFromDB in, BufferedWriter w, String separator)
			throws IOException, SQLException {

		boolean needsNewLine = false;

		if (writeColnamesAsFirstRow && !firstRowWritten) {
			String firstRow = "";
			for (int i = 1; i <= in.getColumnCount(); i++) {
				if (i >= 2)
					firstRow += separator;
				String n = in.getColumnName(i);
				if (n != null)
					n = n.replace("\"", "\\\"");
				firstRow += '"' + n + '"';
			}
			outPrint(firstRow);
			needsNewLine = true;
		}

		for (int rCnt = 0; rCnt < rows.length; rCnt++) {
			Row row = rows[rCnt];
			rows[rCnt] = null; // help the gc. is this necessary?
			if (needsNewLine) {
				long t0 = System.currentTimeMillis();
				outNewline();
				tFileWriteTime += System.currentTimeMillis() - t0;
			}
			rowsExported++;
			StringBuffer b = new StringBuffer(1000);
			boolean needsSeparator = false;
			if (firstColIsRowCounter) {
				b.append(rowsExported).append(separator);
				needsSeparator = true;
			}
			for (int i = 0; i < in.getColumnCount(); i++) {
				if (needsSeparator)
					b.append(separator);
				String s = row.getString(i);
				if (s != null) {
					s = s.replace("\"", "\\\""); // " to \"
					b.append('"').append(s).append('"');
				}
				needsSeparator = true;
			}
			long t0 = System.currentTimeMillis();

			outPrint(b.toString());

			tFileWriteTime += System.currentTimeMillis() - t0;
			needsNewLine = true;
		}

	}

	private void outPrint(String s) throws IOException {
		if (w != null) {
			w.write(s);
		} else {
			System.out.print(s);
			charsWritten += s.length();
		}
	}

	private void outNewline() throws IOException {
		if (w != null)
			w.newLine();
		else
			System.out.println();
		charsWritten++;
	}
}
