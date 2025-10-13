package project1;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import oracle.epm.fm.common.datatype.transport.DATALOAD_DUPLICATE_HANDLING;
import oracle.epm.fm.common.datatype.transport.DATALOAD_FILE_FORMAT;
import oracle.epm.fm.common.datatype.transport.DataLoadOptions;
import oracle.epm.fm.common.datatype.transport.RunningTaskProgress;
import oracle.epm.fm.common.datatype.transport.SessionInfo;
import oracle.epm.fm.common.datatype.transport.USERACTIVITYSTATUS;
import oracle.epm.fm.domainobject.administration.AdministrationOM;
import oracle.epm.fm.domainobject.application.SessionOM;
import oracle.epm.fm.domainobject.loadextract.LoadExtractOM;
import oracle.epm.fm.hssservice.HSSUtilManager;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class HFMload {
    public static void main(String[] args) {
        String username="", password="", appName="", hfmCluster="", dataFile="", delimiter=";";
        SessionOM sessionOM = null; SessionInfo session = null;

        try {
            Options opt = new Options();
            opt.addOption("h", "help", false, "Help");
            opt.addOption("u", "user", true, "Username");
            opt.addOption("p", "password", true, "Password");
            opt.addOption("a", "app", true, "HFM application");
            opt.addOption("c", "cluster", true, "HFM cluster");
            opt.addOption("f", "file", true, "Path to data load file");
            opt.addOption("d", "delimiter", true, "File delimiter (default ;)");

            CommandLine cl = new BasicParser().parse(opt, args);
            if (cl.hasOption('h')) { new HelpFormatter().printHelp("HFMload", opt); System.exit(0); }
            if (cl.hasOption('u')) username = cl.getOptionValue("u"); else { System.err.println("Error: -u required"); System.exit(1); }
            if (cl.hasOption('p')) password = cl.getOptionValue("p"); else { System.err.println("Error: -p required"); System.exit(1); }
            if (cl.hasOption('a')) appName = cl.getOptionValue("a");   else { System.err.println("Error: -a required"); System.exit(1); }
            if (cl.hasOption('c')) hfmCluster = cl.getOptionValue("c"); else { System.err.println("Error: -c required"); System.exit(1); }
            if (cl.hasOption('f')) dataFile = cl.getOptionValue("f");   else { System.err.println("Error: -f required"); System.exit(1); }
            if (cl.hasOption('d')) delimiter = cl.getOptionValue("d");

            System.out.println("Logging in to application " + appName + " with user " + username);
            String ssoToken = HSSUtilManager.getSecurityManager().authenticateUser(username, password);

            sessionOM = new SessionOM();
            session = sessionOM.createSession(ssoToken, Locale.ENGLISH, hfmCluster, appName);

            // Build DataLoadOptions (native format, merge duplicates, LOAD mode)
            List<DataLoadOptions> opts = new ArrayList<DataLoadOptions>();
            DataLoadOptions dl = new DataLoadOptions();
            dl.setAccumulateWithinFile(false);
            dl.setAppendToLogFile(false);
            dl.setContainSharesData(true);
            dl.setContainSubmissionPhaseData(false);
            dl.setDecimalChar("");
            dl.setDelimiter(delimiter);
            dl.setDuplicates(DATALOAD_DUPLICATE_HANDLING.DATALOAD_MERGE);
            dl.setFileFormat(DATALOAD_FILE_FORMAT.DATALOAD_FILE_FORMAT_NATIVE);
            dl.loadCalculated = false;
            dl.setMode(oracle.epm.fm.common.datatype.transport.LOAD_MODE.LOAD);
            dl.setThousandsChar("");
            dl.setUserFileName(dataFile);
            opts.add(dl);

            // Files to load
            ArrayList<String> files = new ArrayList<String>();
            files.add(new File(dataFile).getPath());

            System.out.println("Starting data load: " + dataFile);
            LoadExtractOM loadOM = new LoadExtractOM(session);
            List<Integer> taskIDs = loadOM.loadData(files, opts);

            // Poll until completed
            AdministrationOM adminOM = new AdministrationOM(session);
            boolean completed = false;
            while (!completed) {
                List<RunningTaskProgress> list = adminOM.getCurrentTaskProgress(taskIDs);
                completed = true;
                for (RunningTaskProgress p : list) {
                    System.out.println("Task #" + p.getTaskID() + " - " + p.getDescription().replace("\r\n"," - ")
                        + " : " + p.getPrecentCompleted() + "%");
                    if (p.getTaskStatus() != USERACTIVITYSTATUS.USERACTIVITYSTATUS_COMPLETED) {
                        completed = false;
                    }
                }
                if (!completed) Thread.sleep(2000);
            }
            System.out.println("Data load completed.");

            if (sessionOM != null && session != null) sessionOM.closeSession(session);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            try { if (sessionOM != null && session != null) sessionOM.closeSession(session); } catch (Exception ignore) {}
            System.exit(2);
        }
    }
}
