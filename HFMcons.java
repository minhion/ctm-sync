/*
    HFM Java API Code Example: Run a consolidation

    Author: Henri Vilminko / Infratects
    Date: 2016-06-10

    Sample command line using run.bat:
    run.bat -u admin -p p4ssw0rd -a COMMA4DIM -c SANDBOX -s "S#Actual.Y#2007.P#June;July;August.E#EastRegion"

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

            opt.addOption("h", "help", false,
                          "Print help for this application");
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
            ssoToken =
                    HSSUtilManager.getSecurityManager().authenticateUser(username,
                                                                         password);

            // Create a session to work with an HFM application
            sessionOM = new SessionOM();
            session =
                    sessionOM.createSession(ssoToken, Locale.ENGLISH, hfmCluster,
                                            appName);

            // Create a DataOM object for running data related tasks
            DataOM dataOM = new DataOM(session);

            System.out.println("Starting consolidation...");
            
            // Consolidate the POV given on command line
            List<String> povs = new ArrayList<String>(1);
            povs.add(dataSlice);
            ServerTaskInfo taskInfo =
                dataOM.executeServerTask(WEBOMDATAGRIDTASKMASKENUM.valueOf("WEBOM_DATAGRID_TASK_CONSOLIDATEALLWITHDATA"),
                                         povs);

            // Create an AdministrationOM object for checking task progress
            AdministrationOM adminOM = new AdministrationOM(session);

            // Wait for all started tasks to complete
            Boolean tasksStillRunning = true;
            while (tasksStillRunning) {

                // Update the task progress (returns a list of running tasks)
                List<RunningTaskProgress> listProgress =
                        adminOM.getCurrentTaskProgress(taskInfo.getTaskIDs());

                // Iterate through the list of running tasks,
                // checking status for each.
                for (RunningTaskProgress taskProgress : listProgress) {

                    System.out.println("Task #" + taskProgress.getTaskID() +
                                       ": " + taskProgress.getDescription().replace("\r\n", " - ") +
                                       ", " +
                                       taskProgress.getPrecentCompleted() + "% done");

                    // Keep waiting if any of the tasks are still in the running state
                    USERACTIVITYSTATUS actStatus =
                        taskProgress.getTaskStatus();
                    if (actStatus.equals(USERACTIVITYSTATUS.valueOf("USERACTIVITYSTATUS_RUNNING"))) {
                        tasksStillRunning = true;
                        Thread.sleep(2000);
                    } else {
                        tasksStillRunning = false;
                    }
                }
            }
            System.out.println("All tasks completed!");

            // All tasks done -> close the session gracefully
            if (sessionOM != null && session != null) {
                sessionOM.closeSession(session);
            }

		// Handle any exceptions thrown during the execution (just output the errors)
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());

        }
    }
}
