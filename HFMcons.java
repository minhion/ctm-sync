/*
    HFM Java API Code Example: Run a consolidation

    Original Author: Henri Vilminko / Infratects
    Updated: adds consolidation type switch, safer polling, clearer diagnostics.

    Usage examples:
      java project1.HFMcons -u admin -p p4ssw0rd -a HCHFM -c HCHFMQ -s "S#Actual.Y#2025.P#Aug;Sep.E#CO_J00000"
      java project1.HFMcons --list-types
      java project1.HFMcons -u ... -p ... -a ... -c ... -t allwithdata -s "<POV>"
      java project1.HFMcons -u ... -p ... -a ... -c ... -t impacted -s "<POV>"
      java project1.HFMcons -u ... -p ... -a ... -c ... -t WEBOM_DATAGRID_TASK_CONSOLIDATEALLWITHDATA -s "<POV>"

    Notes:
      - POV = Point-of-View string like: S#Scenario.Y#Year.P#Period(s).E#Entity[.Vw#View][.V#Value][.A#Account][.I#ICP][.C1#...]
      - Default consolidation type = AllWithData (alias: allwithdata)
*/

package project1;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import oracle.epm.fm.common.datatype.transport.RunningTaskProgress;
import oracle.epm.fm.common.datatype.transport.ServerTaskInfo;
import oracle.epm.fm.common.datatype.transport.SessionInfo;
import oracle.epm.fm.common.datatype.transport.USERACTIVITYSTATUS;
import oracle.epm.fm.common.datatype.transport.WEBOMDATAGRIDTASKMASKENUM;
import oracle.epm.fm.domainobject.administration.AdministrationOM;
import oracle.epm.fm.domainobject.application.SessionOM;
import oracle.epm.fm.domainobject.data.DataOM;
import oracle.epm.fm.hssservice.HSSUtilManager;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class HFMcons {

    public static void main(String[] args) {

        String username = "";
        String password = "";
        String appName  = "";
        String ssoToken = "";
        String hfmCluster = "";
        String dataSlice = "";

        // New: consolidation type selection
        String typeArg = "allwithdata"; // default
        boolean listTypes = false;

        SessionOM sessionOM = null;
        SessionInfo session = null;

        try {
            // Parse CLI
            Options opt = new Options();
            opt.addOption("h", "help", false, "Print help for this application");
            opt.addOption("u", "user", true,  "Username for login");
            opt.addOption("p", "password", true, "Password for login");
            opt.addOption("a", "app", true,  "HFM application name");
            opt.addOption("c", "cluster", true, "HFM cluster name");
            opt.addOption("s", "slice", true, "POV data slice (e.g., S#Actual.Y#2025.P#Aug;Sep.E#Entity)");
            opt.addOption("t", "type", true,  "Consolidation type (alias or raw enum). Try: allwithdata|all|impacted|entityonly|force-entityonly");
            opt.addOption(null, "list-types", false, "List available WEBOMDATAGRIDTASKMASKENUM values and exit");

            BasicParser parser = new BasicParser();
            CommandLine cl = parser.parse(opt, args);

            if (cl.hasOption("list-types")) {
                listTypes = true;
            }
            if (cl.hasOption('h')) {
                HelpFormatter f = new HelpFormatter();
                f.printHelp("HFMcons", opt);
                System.exit(0);
            }

            if (cl.hasOption('u')) username   = cl.getOptionValue("u"); else { System.err.println("Error: Username (-u) is required."); System.exit(1); }
            if (cl.hasOption('p')) password   = cl.getOptionValue("p"); else { System.err.println("Error: Password (-p) is required."); System.exit(1); }
            if (cl.hasOption('a')) appName    = cl.getOptionValue("a"); else { System.err.println("Error: Application (-a) is required."); System.exit(1); }
            if (cl.hasOption('c')) hfmCluster = cl.getOptionValue("c"); else { System.err.println("Error: Cluster (-c) is required."); System.exit(1); }
            if (cl.hasOption('s')) dataSlice  = cl.getOptionValue("s");
            if (cl.hasOption('t')) typeArg    = cl.getOptionValue("t");

            if (listTypes) {
                System.out.println("Available WEBOMDATAGRIDTASKMASKENUM values:");
                for (WEBOMDATAGRIDTASKMASKENUM e : WEBOMDATAGRIDTASKMASKENUM.values()) {
                    System.out.println(" - " + e.name());
                }
                System.exit(0);
            }

            // Map friendly aliases to enum; or accept raw enum
            String enumName = mapTypeToEnum(typeArg);
            WEBOMDATAGRIDTASKMASKENUM taskEnum = WEBOMDATAGRIDTASKMASKENUM.valueOf(enumName);

            System.out.println("Logging in to application " + appName + " with user " + username);

            // Login
            ssoToken = HSSUtilManager.getSecurityManager().authenticateUser(username, password);

            // Create session
            sessionOM = new SessionOM();
            session = sessionOM.createSession(ssoToken, Locale.ENGLISH, hfmCluster, appName);

            // Data operations
            DataOM dataOM = new DataOM(session);

            System.out.println("Starting consolidation... Type=" + typeArg + " -> " + taskEnum.name());

            List<String> povs = new ArrayList<String>(1);
            povs.add(dataSlice);

            // Execute consolidation task
            ServerTaskInfo taskInfo = dataOM.executeServerTask(taskEnum, povs);

            // Diagnostics
            System.out.println("DEBUG: taskInfo=" + taskInfo);
            System.out.println("DEBUG: taskIDs=" + (taskInfo == null ? "null" : taskInfo.getTaskIDs()));

            if (taskInfo == null || taskInfo.getTaskIDs() == null || taskInfo.getTaskIDs().isEmpty()) {
                System.err.println("ERROR: No task IDs returned. The POV may be invalid or not consolidatable: " + dataSlice);
                if (sessionOM != null && session != null) sessionOM.closeSession(session);
                System.exit(2);
            }

            // Small delay before first poll (avoids occasional Systeminfo race)
            try { Thread.sleep(5000); } catch (InterruptedException ie) { /* ignore */ }

            AdministrationOM adminOM = new AdministrationOM(session);

            // Poll with limited retries on transient failures
            int pollAttempts = 0;
            final int MAX_POLL_RETRIES = 3;

            boolean tasksStillRunning = true;
            while (tasksStillRunning) {
                List<RunningTaskProgress> listProgress = null;

                try {
                    listProgress = adminOM.getCurrentTaskProgress(taskInfo.getTaskIDs());
                    pollAttempts = 0; // reset on success
                } catch (Exception pollEx) {
                    pollAttempts++;
                    System.err.println("WARN: getCurrentTaskProgress failed (attempt " + pollAttempts + " of " + MAX_POLL_RETRIES + "): " + pollEx.getMessage());
                    if (pollAttempts <= MAX_POLL_RETRIES) {
                        try { Thread.sleep(3000); } catch (InterruptedException ie) { /* ignore */ }
                        continue;
                    } else {
                        throw pollEx;
                    }
                }

                if (listProgress == null || listProgress.isEmpty()) {
                    try { Thread.sleep(2000); } catch (InterruptedException ie) { /* ignore */ }
                    continue;
                }

                tasksStillRunning = false; // assume done unless we see RUNNING
                for (RunningTaskProgress taskProgress : listProgress) {
                    System.out.println(
                        "Task #" + taskProgress.getTaskID() + ": " +
                        taskProgress.getDescription().replace("\r\n", " - ") + ", " +
                        taskProgress.getPrecentCompleted() + "% done"
                    );

                    USERACTIVITYSTATUS actStatus = taskProgress.getTaskStatus();
                    if (USERACTIVITYSTATUS.valueOf("USERACTIVITYSTATUS_RUNNING").equals(actStatus)) {
                        tasksStillRunning = true;
                    }
                }

                if (tasksStillRunning) {
                    try { Thread.sleep(2000); } catch (InterruptedException ie) { /* ignore */ }
                }
            }

            System.out.println("All tasks completed!");

            // Close session gracefully
            if (sessionOM != null && session != null) {
                sessionOM.closeSession(session);
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    // Map friendly -t/--type aliases to your environment's enum names.
    // Use --list-types to see the exact constants available and adjust below if needed.
    private static String mapTypeToEnum(String t) {
        String v = (t == null ? "" : t.trim().toLowerCase(Locale.ENGLISH));

        // Common aliases → typical enum names (adjust if --list-types shows different names)
        if (v.equals("allwithdata") || v.equals("all-with-data") || v.equals("awd")) {
            return "WEBOM_DATAGRID_TASK_CONSOLIDATEALLWITHDATA";
        }
        if (v.equals("all") || v.equals("full")) {
            return "WEBOM_DATAGRID_TASK_CONSOLIDATE";
        }
        if (v.equals("impacted") || v.equals("delta")) {
            return "WEBOM_DATAGRID_TASK_CONSOLIDATEIMPACTED";
        }
        if (v.equals("entityonly") || v.equals("entity-only")) {
            return "WEBOM_DATAGRID_TASK_CONSOLIDATEENTITY";
        }
        if (v.equals("force-entityonly") || v.equals("force_entityonly") || v.equals("forceentityonly")) {
            return "WEBOM_DATAGRID_TASK_CONSOLIDATEFORCEENTITY";
        }

        // Otherwise, assume caller passed a raw enum name (case-insensitive).
        for (WEBOMDATAGRIDTASKMASKENUM e : WEBOMDATAGRIDTASKMASKENUM.values()) {
            if (e.name().equalsIgnoreCase(t)) return e.name();
        }

        // Last resort: return as-is; valueOf() will throw a clear error if wrong.
        return t;
    }
}
