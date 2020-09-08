/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/sqlcp
 */
package com.cg.sqlcp.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedTransferQueue;

import com.cg.sqlutil.Row;

public class Util {

    public static boolean isNull(String s) {
        if (s == null)
            return true;
        if ("".equals(s))
            return true;
        return false;
    }

    public static Integer toInteger(String s) {
        if (isNull(s))
            return null;
        return Integer.parseInt(s);
    }

    public static long getRowCountOfQueue(LinkedTransferQueue<Row[]> q) {
        if (q == null)
            return 0;
        long s = 0;
        for (Row[] rows : q)
            s += rows.length;
        return s;
    }

    public static void log(String s) {
        Date d = new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String msg = "[" + df.format(d) + "] " + s;
        System.out.println(msg);
    }

    public static void log(Throwable t) {
        t.printStackTrace();
    }
}
