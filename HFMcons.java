/*
    HFM Java API Code Example: Run a consolidation

    Author: Henri Vilminko / Infratects
    Date: 2016-06-10

    Notes:
    - Updated by Minh's helper to add:
      * taskInfo/taskIDs sanity print
      * small delay before first poll (avoids Systeminfo race)
      * guarded polling with limited retries for getCurrentTaskProgress
      * clearer error if no task IDs were returned (likely bad POV)

    Sample command line:
    java project1.HFMcons -u admin -p p4ssw0rd -a COMMA4DIM -c SANDBOX -s "S#Actual.Y#2007.P#June;July;August.E#EastRegion"
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
        String appName = "";
        String ssoToken = "";
        String hfmCluster = "";
        String dataSlice = "";
        SessionOM sessionOM = null;
        SessionInfo session = null;

        try {
            // Parse the command line options
            Options opt = new Options();

            opt.addOption("h", "help", false, "Print help for this application");
            opt.addOption("u", "user", true, "Username for login");
            opt.addOption("p", "password", true, "Password for login");
            opt.addOption("a", "app", true, "HFM application name");
            opt.addOption("c", "cluster", true, "HFM cluster name");
            opt.addOption("s", "slice", true, "Data slice definition");

            BasicParser parser = new BasicParser();
            CommandLine cl = parser.parse(opt, args);

            if (cl.hasOption('h')) {
                HelpFormatter f = new HelpFormatter();
                f.printHelp("HFMcons", opt);
                System.exit(0);
            }

            if (cl.hasOption('u')) {
                username = cl.getOptionValue("u");
            } else {
                System.err.println("Error: Username is mandatory (use the -u option).");
                System.exit(1);
            }

            if (cl.hasOption('p')) {
                password = cl.getOptionValue("p");
            } else {
                System.err.println("Error: Password is mandatory (use the -p option).");
                System.exit(1);
            }

            if (cl.hasOption('a')) {
                appName = cl.getOptionValue("a");
            } else {
                System.err.println("Error: Application is mandatory (use the -a option).");
                System.exit(1);
            }

            if (cl.hasOption('c')) {
                hfmCluster = cl.getOptionValue("c");
            } else {
                System.err.println("Error: Cluster is mandatory (use the -c option).");
                System.exit(1);
            }

            if (cl.hasOption('s')) {
                dataSlice = cl.getOptionValue("s");
            }

            System.out.println("Logging in to application " + appName + " with user " + username);

            // Authenticate user to get a security token
            ssoToken = HSSUtilManager.getSecurityManager().authenticateUser(username, password);

            // Create a session to work with an HFM application
            sessionOM = new SessionOM();
            session = sessionOM.createSession(ssoToken, Locale.ENGLISH, hfmCluster, appName);

            // Create a DataOM object for running data related tasks
            DataOM dataOM = new DataOM(session);

            System.out.println("Starting consolidation...");

            // Consolidate the POV given on command line
            List<String> povs = new ArrayList<String>(1);
            povs.add(dataSlice);

            ServerTaskInfo taskInfo = dataOM.executeServerTask(
                WEBOMDATAGRIDTASKMASKENUM.valueOf("WEBOM_DATAGRID_TASK_CONSOLIDATEALLWITHDATA"),
                povs
            );

            // --- Robustness additions start ---
            System.out.println("DEBUG: taskInfo=" + taskInfo);
            System.out.println("DEBUG: taskIDs=" + (taskInfo == null ? "null" : taskInfo.getTaskIDs()));

            if (taskInfo == null || taskInfo.getTaskIDs() == null || taskInfo.getTaskIDs().isEmpty()) {
                System.err.println("ERROR: No task IDs returned. The POV may be invalid or not consolidatable: " + dataSlice);
                // Close session before exit
                if (sessionOM != null && session != null) sessionOM.closeSession(session);
                System.exit(2);
            }

            // Small delay to avoid Systeminfo handler race on first poll
            try { Thread.sleep(5000); } catch (InterruptedException ie) { /* ignore */ }

            AdministrationOM adminOM = new AdministrationOM(session);

            // Guarded polling with a few retries if Systeminfo briefly fails
            int pollAttempts = 0;
            final int MAX_POLL_RETRIES = 3;

            Boolean tasksStillRunning = true;
            while (tasksStillRunning) {

                List<RunningTaskProgress> listProgress = null;

                try {
                    listProgress = adminOM.getCurrentTaskProgress(taskInfo.getTaskIDs());
                    // reset attempts on success
                    pollAttempts = 0;
                } catch (Exception pollEx) {
                    pollAttempts++;
                    System.err.println("WARN: getCurrentTaskProgress failed (attempt " + pollAttempts + " of " + MAX_POLL_RETRIES + "): " + pollEx.getMessage());
                    if (pollAttempts <= MAX_POLL_RETRIES) {
                        try { Thread.sleep(3000); } catch (InterruptedException ie) { /* ignore */ }
                        continue; // retry
                    } else {
                        throw pollEx; // give up with original error
                    }
                }

                // If server returned no progress entries, keep a short wait and continue one cycle
                if (listProgress == null || listProgress.isEmpty()) {
                    try { Thread.sleep(2000); } catch (InterruptedException ie) { /* ignore */ }
                    continue;
                }

                // Iterate through the list of running tasks, checking status for each.
                tasksStillRunning = false; // assume done unless we find RUNNING
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
            // --- Robustness additions end ---

            System.out.println("All tasks completed!");

            // All tasks done -> close the session gracefully
            if (sessionOM != null && session != null) {
                sessionOM.closeSession(session);
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
