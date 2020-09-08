/*
 * Author Christian Gausepohl
 * License: CC0 (no copyright if possible, otherwise fallback to public domain)
 * https://github.com/cgausepohl/sqlcp
 */
package com.cg.sqlcp;

import java.io.IOException;
import java.sql.SQLException;

import com.cg.sqlcp.commands.ExportDB2DB;
import com.cg.sqlcp.commands.ExportDB2File;

/*
CURRENT
=======

TODO
====
 1: postgres: password seems to be useless
 2: check target table to exist
 4: params erweitern bei toDB: ZielSpalten bzw. insert in eine tabelle mit mehr spalten als source
26: log all sql-stmts(log file or std out)
 3: oracle type mapper, try to detect int and long and bigint
 5: params -logmode=silent/verbose   silent(only_errors)
 6: types implementieren für alle toDB inserts
 7: check if batchUpdate possible, if not then single insert
 8: check environment and make suggestions (driver type, same db type, same db)
 9: testmatrix: types*database int, float, string, text, date, time, datetime, interval, blob, xml, json
10: mysql_mariadb+oracle+mongo+redis+mssql
11: option:pre/after-script für src+dest, mit möglichkeit zur ausgabe auf stdout
27: git: add license header to every file

DEAD?
======
12: add numberformatter for integers
13: es fehlen zeilen beim toDB
14: mit select * toDB

DONE
===
15: quote some characters during filewriter (data & headline)
17: use SQLUtil for read and bulk write
18: add streaming
19: use streaming in atomic gets, only one get-logic at the end 
20: sqlutil-stream fähig machen ODER sqlutil raus
21: option:commit after each chunck
22: logging (err/stdout)
23: aufruf des programs ins log, dann alle gesetzten parameter (um default werte zu sehen)
24: CLI flags raus, alles einfacher
25: monitoring: memory in/out, chunksizes, wait-times OR idle-times, network-in/network-out for src+dest
*/

public final class SQLcp {

    public static void printMainHelp() {
        System.out.println("SQLcp <db2db | db2file | file2db>");
        System.out.println("  db2db: read from source database, write into target database");
        System.out.println("  db2file: read from source database, write to target file");
        System.out.println("  (not yet implemented) file2db: read from source file, write to target database");
        System.exit(1);
    }

    public void start(String[] args) throws InterruptedException, SQLException, IOException {
        if (args.length >= 1 && ("db2db".equals(args[0]))) {
            new ExportDB2DB().start(args);
        } else if (args.length >= 1 && "db2file".equals(args[0])) {
            new ExportDB2File().start(args);
        } else {
            printMainHelp();
        }
    }

    public static void main(String[] args) throws Exception {
        // System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF
        // %1$tl:%1$tM:%1$tS.%1$tL (%4$s) [%2$s] %5$s%6$s%n");
        new SQLcp().start(args);
    }
}
