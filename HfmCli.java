/*
 * HfmCli.java - Unified HFM Command Line Interface for Control-M Integration
 * 
 * Author: Adapted from Henri Vilminko / Infratects examples
 * Purpose: Single entry point for all HFM operations callable from Control-M
 * 
 * Supported Operations:
 *   - Consolidate
 *   - LoadData
 *   - Translate
 *   - ExtractDataToFlatfile
 *   - ExtractDataToDatabase
 *   - ExtractMetadata
 *   - ExtractRules
 *   - ExtractMemberLists
 *   - ExtractSecurity
 *   - ExtractJournals
 * 
 * Usage:
 *   java project1.HfmCli <operation> [options]
 * 
 * Example:
 *   java project1.HfmCli Consolidate -u admin -p password -a HCHFM -c HCHFMP -s "S#Actual.Y#2025..." -t AllWithData
 */

package project1;

import java.io.File;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import oracle.epm.fm.common.datatype.transport.*;
import oracle.epm.fm.domainobject.administration.AdministrationOM;
import oracle.epm.fm.domainobject.application.SessionOM;
import oracle.epm.fm.domainobject.data.DataOM;
import oracle.epm.fm.domainobject.loadextract.LoadExtractOM;
import oracle.epm.fm.domainobject.metadata.MetadataOM;
import oracle.epm.fm.hssservice.HSSUtilManager;

import org.apache.commons.cli.*;

public class HfmCli {

    // ==================== Constants ====================
    private static final String VERSION = "2.0.0";
    private static final int DEFAULT_POLL_INTERVAL = 2000; // milliseconds
    
    // Exit codes for Control-M
    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_INVALID_ARGS = 1;
    private static final int EXIT_AUTH_FAILED = 2;
    private static final int EXIT_OPERATION_FAILED = 3;
    private static final int EXIT_TASK_FAILED = 4;
    private static final int EXIT_UNKNOWN_ERROR = 5;

    // ==================== JSON Output Helpers ====================
    
    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
    
    private static void jsonOutput(PrintStream out, String status, String message, 
            String operation, String application, long elapsedMs, List<Integer> taskIds) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"status\":\"").append(status).append("\"");
        sb.append(",\"message\":\"").append(escapeJson(message)).append("\"");
        sb.append(",\"operation\":\"").append(escapeJson(operation)).append("\"");
        sb.append(",\"application\":\"").append(escapeJson(application)).append("\"");
        sb.append(",\"elapsed_ms\":").append(elapsedMs);
        sb.append(",\"timestamp\":\"").append(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").format(new Date())).append("\"");
        if (taskIds != null && !taskIds.isEmpty()) {
            sb.append(",\"task_ids\":[");
            for (int i = 0; i < taskIds.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(taskIds.get(i));
            }
            sb.append("]");
        }
        sb.append("}");
        out.println(sb.toString());
    }
    
    private static void jsonError(String message, String operation, String application) {
        jsonOutput(System.out, "Error", message, operation, application, 0, null);
    }
    
    private static void jsonSuccess(String message, String operation, String application, 
            long elapsedMs, List<Integer> taskIds) {
        jsonOutput(System.out, "OK", message, operation, application, elapsedMs, taskIds);
    }
    
    private static void jsonProgress(int taskId, String description, int percent, String status) {
        System.out.println("{\"type\":\"progress\",\"task_id\":" + taskId + 
                ",\"description\":\"" + escapeJson(description) + 
                "\",\"percent\":" + percent + 
                ",\"status\":\"" + escapeJson(status) + "\"}");
    }

    // ==================== Session Management ====================
    
    private static class HfmSession {
        SessionOM sessionOM;
        SessionInfo sessionInfo;
        String ssoToken;
        
        void close() {
            try {
                if (sessionOM != null && sessionInfo != null) {
                    sessionOM.closeSession(sessionInfo);
                }
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }
    
    private static HfmSession createSession(String username, String password, 
            String cluster, String application) throws Exception {
        HfmSession session = new HfmSession();
        
        // Authenticate
        session.ssoToken = HSSUtilManager.getSecurityManager()
                .authenticateUser(username, password);
        
        // Create session
        session.sessionOM = new SessionOM();
        session.sessionInfo = session.sessionOM.createSession(
                session.ssoToken, Locale.ENGLISH, cluster, application);
        
        return session;
    }

    // ==================== Task Monitoring ====================
    
    private static boolean waitForTasks(HfmSession session, List<Integer> taskIds, 
            int pollInterval, boolean verbose) throws Exception {
        
        AdministrationOM adminOM = new AdministrationOM(session.sessionInfo);
        boolean allCompleted = false;
        boolean anyFailed = false;
        
        while (!allCompleted) {
            List<RunningTaskProgress> progressList = adminOM.getCurrentTaskProgress(taskIds);
            allCompleted = true;
            
            for (RunningTaskProgress progress : progressList) {
                USERACTIVITYSTATUS status = progress.getTaskStatus();
                String desc = progress.getDescription();
                if (desc != null) desc = desc.replace("\r\n", " - ");
                
                if (verbose) {
                    jsonProgress(progress.getTaskID(), desc, 
                            progress.getPrecentCompleted(), status.toString());
                }
                
                // Check if task is still in progress
                if (status == USERACTIVITYSTATUS.USERACTIVITYSTATUS_RUNNING ||
                    status == USERACTIVITYSTATUS.USERACTIVITYSTATUS_STARTING ||
                    status == USERACTIVITYSTATUS.USERACTIVITYSTATUS_SCHEDULED_START ||
                    status == USERACTIVITYSTATUS.USERACTIVITYSTATUS_SCHEDULED_STOP) {
                    allCompleted = false;
                } else if (status == USERACTIVITYSTATUS.USERACTIVITYSTATUS_ABORTED ||
                           status == USERACTIVITYSTATUS.USERACTIVITYSTATUS_STOPPED) {
                    // USERACTIVITYSTATUS_ABORTED = task failed/was aborted
                    // USERACTIVITYSTATUS_STOPPED = task was stopped by user
                    anyFailed = true;
                }
            }
            
            if (!allCompleted) {
                Thread.sleep(pollInterval);
            }
        }
        
        return !anyFailed;
    }
    
    // For ServerTaskInfo (consolidate/translate operations)
    private static boolean waitForServerTask(HfmSession session, ServerTaskInfo taskInfo,
            int pollInterval, boolean verbose) throws Exception {
        List<Integer> taskIds = taskInfo.getTaskIDs();
        return waitForTasks(session, taskIds, pollInterval, verbose);
    }

    // ==================== Consolidation Types Mapping ====================
    
    private static WEBOMDATAGRIDTASKMASKENUM getConsolidationType(String type) {
        if (type == null) type = "AllWithData";
        
        switch (type.toLowerCase().replace(" ", "").replace("_", "")) {
            case "allwithdata":
                return WEBOMDATAGRIDTASKMASKENUM.WEBOM_DATAGRID_TASK_CONSOLIDATEALLWITHDATA;
            case "all":
                return WEBOMDATAGRIDTASKMASKENUM.WEBOM_DATAGRID_TASK_CONSOLIDATEALL;
            case "impacted":
                return WEBOMDATAGRIDTASKMASKENUM.WEBOM_DATAGRID_TASK_CONSOLIDATE;
            case "allwithdataforcecalculate":
            case "allwithdata+force":
                // Note: WEBOM_DATAGRID_TASK_CONSOLIDATEALLWITHDATA_FORCECALCULATE doesn't exist in 11.1.2.0
                // Use WEBOM_DATAGRID_TASK_FORCECALCULATE for force calculate operations
                return WEBOMDATAGRIDTASKMASKENUM.WEBOM_DATAGRID_TASK_FORCECALCULATE;
            default:
                return WEBOMDATAGRIDTASKMASKENUM.WEBOM_DATAGRID_TASK_CONSOLIDATEALLWITHDATA;
        }
    }

    // ==================== Operation: Consolidate ====================
    
    private static int doConsolidate(CommandLine cl, int pollInterval, boolean verbose) {
        long startTime = System.currentTimeMillis();
        String application = cl.getOptionValue("a");
        HfmSession session = null;
        
        try {
            String username = cl.getOptionValue("u");
            String password = cl.getOptionValue("p");
            String cluster = cl.getOptionValue("c");
            String pov = cl.getOptionValue("s");
            String type = cl.getOptionValue("t", "AllWithData");
            
            // Validate required parameters
            if (username == null || password == null || application == null || 
                cluster == null || pov == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c, -s", "Consolidate", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Execute consolidation
            DataOM dataOM = new DataOM(session.sessionInfo);
            List<String> povList = new ArrayList<>();
            povList.add(pov);
            
            WEBOMDATAGRIDTASKMASKENUM consolidationType = getConsolidationType(type);
            ServerTaskInfo taskInfo = dataOM.executeServerTask(consolidationType, povList);
            
            // Wait for completion
            List<Integer> taskIds = taskInfo.getTaskIDs();
            boolean success = waitForServerTask(session, taskInfo, pollInterval, verbose);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            if (success) {
                jsonSuccess("Consolidation completed successfully", "Consolidate", 
                        application, elapsed, taskIds);
                return EXIT_SUCCESS;
            } else {
                jsonOutput(System.out, "Failed", "One or more consolidation tasks failed", 
                        "Consolidate", application, elapsed, taskIds);
                return EXIT_TASK_FAILED;
            }
            
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            jsonOutput(System.out, "Error", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "Consolidate", application, elapsed, null);
            if (verbose) e.printStackTrace(System.err);
            return EXIT_OPERATION_FAILED;
        } finally {
            if (session != null) session.close();
        }
    }

    // ==================== Operation: Load Data ====================
    
    private static int doLoadData(CommandLine cl, int pollInterval, boolean verbose) {
        long startTime = System.currentTimeMillis();
        String application = cl.getOptionValue("a");
        HfmSession session = null;
        
        try {
            String username = cl.getOptionValue("u");
            String password = cl.getOptionValue("p");
            String cluster = cl.getOptionValue("c");
            String dataFile = cl.getOptionValue("f");
            String delimiter = cl.getOptionValue("d", ";");
            String loadMode = cl.getOptionValue("loadMode", "Merge");
            boolean accumulate = Boolean.parseBoolean(cl.getOptionValue("accumulate", "false"));
            boolean scanOnly = Boolean.parseBoolean(cl.getOptionValue("scanOnly", "false"));
            
            // Validate required parameters
            if (username == null || password == null || application == null || 
                cluster == null || dataFile == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c, -f", "LoadData", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Build load options
            DataLoadOptions options = new DataLoadOptions();
            options.setDelimiter(delimiter);
            options.setAccumulateWithinFile(accumulate);
            options.setUserFileName(dataFile);
            options.setAppendToLogFile(false);
            options.setContainSharesData(true);
            options.setContainSubmissionPhaseData(false);
            options.setDecimalChar("");
            options.setThousandsChar("");
            options.loadCalculated = false;
            
            // Set load mode
            switch (loadMode.toLowerCase()) {
                case "merge":
                    options.setDuplicates(DATALOAD_DUPLICATE_HANDLING.DATALOAD_MERGE);
                    options.setMode(LOAD_MODE.LOAD);
                    break;
                case "replace":
                    options.setDuplicates(DATALOAD_DUPLICATE_HANDLING.DATALOAD_REPLACE);
                    options.setMode(LOAD_MODE.LOAD);
                    break;
                case "accumulate":
                    options.setDuplicates(DATALOAD_DUPLICATE_HANDLING.DATALOAD_ACCUMULATE);
                    options.setMode(LOAD_MODE.LOAD);
                    break;
                default:
                    options.setDuplicates(DATALOAD_DUPLICATE_HANDLING.DATALOAD_MERGE);
                    options.setMode(LOAD_MODE.LOAD);
            }
            
            options.setFileFormat(DATALOAD_FILE_FORMAT.DATALOAD_FILE_FORMAT_NATIVE);
            
            List<DataLoadOptions> optsList = new ArrayList<>();
            optsList.add(options);
            
            List<String> files = new ArrayList<>();
            files.add(new File(dataFile).getPath());
            
            // Execute load
            LoadExtractOM loadOM = new LoadExtractOM(session.sessionInfo);
            List<Integer> taskIds = loadOM.loadData(files, optsList);
            
            // Wait for completion
            boolean success = waitForTasks(session, taskIds, pollInterval, verbose);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            if (success) {
                jsonSuccess("Data load completed successfully", "LoadData", 
                        application, elapsed, taskIds);
                return EXIT_SUCCESS;
            } else {
                jsonOutput(System.out, "Failed", "Data load task failed", 
                        "LoadData", application, elapsed, taskIds);
                return EXIT_TASK_FAILED;
            }
            
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            jsonOutput(System.out, "Error", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "LoadData", application, elapsed, null);
            if (verbose) e.printStackTrace(System.err);
            return EXIT_OPERATION_FAILED;
        } finally {
            if (session != null) session.close();
        }
    }

    // ==================== Operation: Translate ====================
    
    private static int doTranslate(CommandLine cl, int pollInterval, boolean verbose) {
        long startTime = System.currentTimeMillis();
        String application = cl.getOptionValue("a");
        HfmSession session = null;
        
        try {
            String username = cl.getOptionValue("u");
            String password = cl.getOptionValue("p");
            String cluster = cl.getOptionValue("c");
            String pov = cl.getOptionValue("s");
            boolean force = Boolean.parseBoolean(cl.getOptionValue("force", "false"));
            
            // Validate required parameters
            if (username == null || password == null || application == null || 
                cluster == null || pov == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c, -s", "Translate", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Execute translation
            DataOM dataOM = new DataOM(session.sessionInfo);
            List<String> povList = new ArrayList<>();
            povList.add(pov);
            
            WEBOMDATAGRIDTASKMASKENUM translateType = force ? 
                    WEBOMDATAGRIDTASKMASKENUM.WEBOM_DATAGRID_TASK_FORCETRANSLATE :
                    WEBOMDATAGRIDTASKMASKENUM.WEBOM_DATAGRID_TASK_TRANSLATE;
            
            ServerTaskInfo taskInfo = dataOM.executeServerTask(translateType, povList);
            
            // Wait for completion
            List<Integer> taskIds = taskInfo.getTaskIDs();
            boolean success = waitForServerTask(session, taskInfo, pollInterval, verbose);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            if (success) {
                jsonSuccess("Translation completed successfully", "Translate", 
                        application, elapsed, taskIds);
                return EXIT_SUCCESS;
            } else {
                jsonOutput(System.out, "Failed", "Translation task failed", 
                        "Translate", application, elapsed, taskIds);
                return EXIT_TASK_FAILED;
            }
            
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            jsonOutput(System.out, "Error", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "Translate", application, elapsed, null);
            if (verbose) e.printStackTrace(System.err);
            return EXIT_OPERATION_FAILED;
        } finally {
            if (session != null) session.close();
        }
    }

    // ==================== Operation: Extract Data to Flatfile ====================
    
    private static int doExtractDataToFlatfile(CommandLine cl, int pollInterval, boolean verbose) {
        long startTime = System.currentTimeMillis();
        String application = cl.getOptionValue("a");
        HfmSession session = null;
        
        try {
            String username = cl.getOptionValue("u");
            String password = cl.getOptionValue("p");
            String cluster = cl.getOptionValue("c");
            String pov = cl.getOptionValue("s");
            String dataFile = cl.getOptionValue("f");
            String delimiter = cl.getOptionValue("d", ";");
            String extractFormat = cl.getOptionValue("extractFormat", "Flatfile - without header");
            String lineItemOption = cl.getOptionValue("lineItemOption", "None");
            boolean calculatedData = Boolean.parseBoolean(cl.getOptionValue("calculatedData", "false"));
            boolean derivedData = Boolean.parseBoolean(cl.getOptionValue("derivedData", "false"));
            boolean dynamicAccounts = Boolean.parseBoolean(cl.getOptionValue("dynamicAccounts", "false"));
            
            // Validate required parameters
            if (username == null || password == null || application == null || 
                cluster == null || pov == null || dataFile == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c, -s, -f", 
                        "ExtractDataToFlatfile", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Build extract options
            DataExtractOptions options = new DataExtractOptions();
            options.setDelimiter(delimiter);
            options.setIncludeCalculatedData(calculatedData);
            options.setIncludeDerivedData(derivedData);
            options.setIncludeDynamicAccounts(dynamicAccounts);
            // Note: File path is typically passed to extractData method, not options
            
            // Set extract format using DATA_EXTRACT_TYPE_FLAG enum
            if (extractFormat.toLowerCase().contains("without header")) {
                options.setExtractFormat(DATA_EXTRACT_TYPE_FLAG.DATA_EXTRACT_TYPE_FLAG_FLATFILE_NOHEADER);
            } else if (extractFormat.toLowerCase().contains("with header")) {
                options.setExtractFormat(DATA_EXTRACT_TYPE_FLAG.DATA_EXTRACT_TYPE_FLAG_FLATFILE_HEADER);
            } else {
                options.setExtractFormat(DATA_EXTRACT_TYPE_FLAG.DATA_EXTRACT_TYPE_FLAG_NATIVE);
            }
            
            List<String> povList = new ArrayList<>();
            povList.add(pov);
            
            // Execute extract
            LoadExtractOM extractOM = new LoadExtractOM(session.sessionInfo);
            List<Integer> taskIds = extractOM.extractData(povList, options);
            
            // Wait for completion
            boolean success = waitForTasks(session, taskIds, pollInterval, verbose);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            if (success) {
                jsonSuccess("Data extract completed successfully", "ExtractDataToFlatfile", 
                        application, elapsed, taskIds);
                return EXIT_SUCCESS;
            } else {
                jsonOutput(System.out, "Failed", "Data extract task failed", 
                        "ExtractDataToFlatfile", application, elapsed, taskIds);
                return EXIT_TASK_FAILED;
            }
            
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            jsonOutput(System.out, "Error", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "ExtractDataToFlatfile", application, elapsed, null);
            if (verbose) e.printStackTrace(System.err);
            return EXIT_OPERATION_FAILED;
        } finally {
            if (session != null) session.close();
        }
    }

    // ==================== Operation: Extract Data to Database ====================
    
    private static int doExtractDataToDatabase(CommandLine cl, int pollInterval, boolean verbose) {
        long startTime = System.currentTimeMillis();
        String application = cl.getOptionValue("a");
        HfmSession session = null;
        
        try {
            String username = cl.getOptionValue("u");
            String password = cl.getOptionValue("p");
            String cluster = cl.getOptionValue("c");
            String pov = cl.getOptionValue("s");
            String dsn = cl.getOptionValue("dsn");
            String prefix = cl.getOptionValue("prefix", "HFM");
            String schemaAction = cl.getOptionValue("schemaAction", "Create Star Schema");
            boolean calculatedData = Boolean.parseBoolean(cl.getOptionValue("calculatedData", "false"));
            boolean derivedData = Boolean.parseBoolean(cl.getOptionValue("derivedData", "false"));
            
            // Validate required parameters
            if (username == null || password == null || application == null || 
                cluster == null || pov == null || dsn == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c, -s, --dsn", 
                        "ExtractDataToDatabase", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Build extract options for database
            DataExtractOptions options = new DataExtractOptions();
            options.setIncludeCalculatedData(calculatedData);
            options.setIncludeDerivedData(derivedData);
            options.setDSN(dsn);
            options.setTablePrefix(prefix);
            
            // Set extract format for database
            options.setExtractFormat(DATA_EXTRACT_TYPE_FLAG.DATA_EXTRACT_TYPE_FLAG_DATABASE);
            
            List<String> povList = new ArrayList<>();
            povList.add(pov);
            
            // Execute extract
            LoadExtractOM extractOM = new LoadExtractOM(session.sessionInfo);
            List<Integer> taskIds = extractOM.extractData(povList, options);
            
            // Wait for completion
            boolean success = waitForTasks(session, taskIds, pollInterval, verbose);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            if (success) {
                jsonSuccess("Database extract completed successfully", "ExtractDataToDatabase", 
                        application, elapsed, taskIds);
                return EXIT_SUCCESS;
            } else {
                jsonOutput(System.out, "Failed", "Database extract task failed", 
                        "ExtractDataToDatabase", application, elapsed, taskIds);
                return EXIT_TASK_FAILED;
            }
            
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            jsonOutput(System.out, "Error", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "ExtractDataToDatabase", application, elapsed, null);
            if (verbose) e.printStackTrace(System.err);
            return EXIT_OPERATION_FAILED;
        } finally {
            if (session != null) session.close();
        }
    }

    // ==================== Operation: Extract Metadata ====================
    
    private static int doExtractMetadata(CommandLine cl, int pollInterval, boolean verbose) {
        long startTime = System.currentTimeMillis();
        String application = cl.getOptionValue("a");
        HfmSession session = null;
        
        try {
            String username = cl.getOptionValue("u");
            String password = cl.getOptionValue("p");
            String cluster = cl.getOptionValue("c");
            String dataFile = cl.getOptionValue("f");
            String delimiter = cl.getOptionValue("d", ";");
            String fileFormat = cl.getOptionValue("fileFormat", "app");
            
            // Metadata selection options
            boolean accounts = Boolean.parseBoolean(cl.getOptionValue("accounts", "false"));
            boolean entities = Boolean.parseBoolean(cl.getOptionValue("entities", "false"));
            boolean scenarios = Boolean.parseBoolean(cl.getOptionValue("scenarios", "false"));
            boolean icps = Boolean.parseBoolean(cl.getOptionValue("ICPs", "false"));
            boolean currencies = Boolean.parseBoolean(cl.getOptionValue("currencies", "false"));
            boolean values = Boolean.parseBoolean(cl.getOptionValue("values", "false"));
            boolean appSettings = Boolean.parseBoolean(cl.getOptionValue("applicationSettings", "false"));
            boolean consolidationMethods = Boolean.parseBoolean(cl.getOptionValue("consolidationMethods", "false"));
            boolean cellTextLabels = Boolean.parseBoolean(cl.getOptionValue("cellTextLabels", "false"));
            boolean systemAccounts = Boolean.parseBoolean(cl.getOptionValue("systemAccounts", "false"));
            String customDimensions = cl.getOptionValue("customDimensions", "");
            
            // Validate required parameters
            if (username == null || password == null || application == null || 
                cluster == null || dataFile == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c, -f", 
                        "ExtractMetadata", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Build metadata extract options
            MetadataExtractOptions options = new MetadataExtractOptions();
            options.setDelimiter(delimiter);
            options.setUserFileName(dataFile);
            options.setAccounts(accounts);
            options.setEntities(entities);
            options.setScenarios(scenarios);
            options.setICPs(icps);
            options.setCurrencies(currencies);
            options.setValues(values);
            options.setApplicationSettings(appSettings);
            options.setConsolidationMethods(consolidationMethods);
            options.setCellTextLabels(cellTextLabels);
            options.setSystemAccounts(systemAccounts);
            
            // Custom dimensions
            if (customDimensions != null && !customDimensions.isEmpty()) {
                String[] dims = customDimensions.split("[;,]");
                for (String dim : dims) {
                    if (!dim.trim().isEmpty()) {
                        options.addCustomDimension(dim.trim());
                    }
                }
            }
            
            // Set file format
            if (fileFormat.equalsIgnoreCase("xml")) {
                options.setFileFormat(METADATA_FILE_FORMAT.METADATA_FILE_FORMAT_XML);
            } else {
                options.setFileFormat(METADATA_FILE_FORMAT.METADATA_FILE_FORMAT_APP);
            }
            
            // Execute extract
            LoadExtractOM extractOM = new LoadExtractOM(session.sessionInfo);
            List<Integer> taskIds = extractOM.extractMetadata(options);
            
            // Wait for completion
            boolean success = waitForTasks(session, taskIds, pollInterval, verbose);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            if (success) {
                jsonSuccess("Metadata extract completed successfully", "ExtractMetadata", 
                        application, elapsed, taskIds);
                return EXIT_SUCCESS;
            } else {
                jsonOutput(System.out, "Failed", "Metadata extract task failed", 
                        "ExtractMetadata", application, elapsed, taskIds);
                return EXIT_TASK_FAILED;
            }
            
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            jsonOutput(System.out, "Error", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "ExtractMetadata", application, elapsed, null);
            if (verbose) e.printStackTrace(System.err);
            return EXIT_OPERATION_FAILED;
        } finally {
            if (session != null) session.close();
        }
    }

    // ==================== Operation: Extract Rules ====================
    
    private static int doExtractRules(CommandLine cl, int pollInterval, boolean verbose) {
        long startTime = System.currentTimeMillis();
        String application = cl.getOptionValue("a");
        HfmSession session = null;
        
        try {
            String username = cl.getOptionValue("u");
            String password = cl.getOptionValue("p");
            String cluster = cl.getOptionValue("c");
            String dataFile = cl.getOptionValue("f");
            
            // Validate required parameters
            if (username == null || password == null || application == null || 
                cluster == null || dataFile == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c, -f", 
                        "ExtractRules", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Execute extract
            LoadExtractOM extractOM = new LoadExtractOM(session.sessionInfo);
            List<Integer> taskIds = extractOM.extractRules(dataFile);
            
            // Wait for completion
            boolean success = waitForTasks(session, taskIds, pollInterval, verbose);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            if (success) {
                jsonSuccess("Rules extract completed successfully", "ExtractRules", 
                        application, elapsed, taskIds);
                return EXIT_SUCCESS;
            } else {
                jsonOutput(System.out, "Failed", "Rules extract task failed", 
                        "ExtractRules", application, elapsed, taskIds);
                return EXIT_TASK_FAILED;
            }
            
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            jsonOutput(System.out, "Error", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "ExtractRules", application, elapsed, null);
            if (verbose) e.printStackTrace(System.err);
            return EXIT_OPERATION_FAILED;
        } finally {
            if (session != null) session.close();
        }
    }

    // ==================== Operation: Extract Member Lists ====================
    
    private static int doExtractMemberLists(CommandLine cl, int pollInterval, boolean verbose) {
        long startTime = System.currentTimeMillis();
        String application = cl.getOptionValue("a");
        HfmSession session = null;
        
        try {
            String username = cl.getOptionValue("u");
            String password = cl.getOptionValue("p");
            String cluster = cl.getOptionValue("c");
            String dataFile = cl.getOptionValue("f");
            
            // Validate required parameters
            if (username == null || password == null || application == null || 
                cluster == null || dataFile == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c, -f", 
                        "ExtractMemberLists", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Execute extract
            LoadExtractOM extractOM = new LoadExtractOM(session.sessionInfo);
            List<Integer> taskIds = extractOM.extractMemberLists(dataFile);
            
            // Wait for completion
            boolean success = waitForTasks(session, taskIds, pollInterval, verbose);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            if (success) {
                jsonSuccess("Member lists extract completed successfully", "ExtractMemberLists", 
                        application, elapsed, taskIds);
                return EXIT_SUCCESS;
            } else {
                jsonOutput(System.out, "Failed", "Member lists extract task failed", 
                        "ExtractMemberLists", application, elapsed, taskIds);
                return EXIT_TASK_FAILED;
            }
            
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            jsonOutput(System.out, "Error", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "ExtractMemberLists", application, elapsed, null);
            if (verbose) e.printStackTrace(System.err);
            return EXIT_OPERATION_FAILED;
        } finally {
            if (session != null) session.close();
        }
    }

    // ==================== Operation: Extract Security ====================
    
    private static int doExtractSecurity(CommandLine cl, int pollInterval, boolean verbose) {
        long startTime = System.currentTimeMillis();
        String application = cl.getOptionValue("a");
        HfmSession session = null;
        
        try {
            String username = cl.getOptionValue("u");
            String password = cl.getOptionValue("p");
            String cluster = cl.getOptionValue("c");
            String dataFile = cl.getOptionValue("f");
            String delimiter = cl.getOptionValue("d", ";");
            
            boolean securityClasses = Boolean.parseBoolean(cl.getOptionValue("securityClasses", "true"));
            boolean roleAccess = Boolean.parseBoolean(cl.getOptionValue("roleAccess", "true"));
            boolean securityClassAccess = Boolean.parseBoolean(cl.getOptionValue("securityClassAccess", "true"));
            boolean loadUsers = Boolean.parseBoolean(cl.getOptionValue("loadUsers", "true"));
            
            // Validate required parameters
            if (username == null || password == null || application == null || 
                cluster == null || dataFile == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c, -f", 
                        "ExtractSecurity", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Build security extract options
            SecurityExtractOptions options = new SecurityExtractOptions();
            options.setDelimiter(delimiter);
            options.setUserFileName(dataFile);
            options.setSecurityClasses(securityClasses);
            options.setRoleAccess(roleAccess);
            options.setSecurityClassAccess(securityClassAccess);
            options.setUsers(loadUsers);
            
            // Execute extract
            LoadExtractOM extractOM = new LoadExtractOM(session.sessionInfo);
            List<Integer> taskIds = extractOM.extractSecurity(options);
            
            // Wait for completion
            boolean success = waitForTasks(session, taskIds, pollInterval, verbose);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            if (success) {
                jsonSuccess("Security extract completed successfully", "ExtractSecurity", 
                        application, elapsed, taskIds);
                return EXIT_SUCCESS;
            } else {
                jsonOutput(System.out, "Failed", "Security extract task failed", 
                        "ExtractSecurity", application, elapsed, taskIds);
                return EXIT_TASK_FAILED;
            }
            
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            jsonOutput(System.out, "Error", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "ExtractSecurity", application, elapsed, null);
            if (verbose) e.printStackTrace(System.err);
            return EXIT_OPERATION_FAILED;
        } finally {
            if (session != null) session.close();
        }
    }

    // ==================== Operation: Extract Journals ====================
    
    private static int doExtractJournals(CommandLine cl, int pollInterval, boolean verbose) {
        long startTime = System.currentTimeMillis();
        String application = cl.getOptionValue("a");
        HfmSession session = null;
        
        try {
            String username = cl.getOptionValue("u");
            String password = cl.getOptionValue("p");
            String cluster = cl.getOptionValue("c");
            String pov = cl.getOptionValue("s");
            String dataFile = cl.getOptionValue("f");
            String delimiter = cl.getOptionValue("d", ";");
            
            // Journal status options
            boolean extractJournals = Boolean.parseBoolean(cl.getOptionValue("extractJournals", "true"));
            boolean extractTemplates = Boolean.parseBoolean(cl.getOptionValue("extractTemplates", "true"));
            boolean posted = Boolean.parseBoolean(cl.getOptionValue("posted", "true"));
            boolean approved = Boolean.parseBoolean(cl.getOptionValue("approved", "true"));
            boolean submitted = Boolean.parseBoolean(cl.getOptionValue("submitted", "true"));
            boolean rejected = Boolean.parseBoolean(cl.getOptionValue("rejected", "true"));
            boolean working = Boolean.parseBoolean(cl.getOptionValue("working", "true"));
            boolean regular = Boolean.parseBoolean(cl.getOptionValue("regular", "true"));
            boolean autoreversing = Boolean.parseBoolean(cl.getOptionValue("autoreversing", "true"));
            boolean autoreversal = Boolean.parseBoolean(cl.getOptionValue("autoreversal", "true"));
            boolean recurringTemplates = Boolean.parseBoolean(cl.getOptionValue("recurringTemplates", "true"));
            boolean balanced = Boolean.parseBoolean(cl.getOptionValue("balanced", "true"));
            boolean unbalanced = Boolean.parseBoolean(cl.getOptionValue("unbalanced", "true"));
            boolean balancedByEntity = Boolean.parseBoolean(cl.getOptionValue("balancedByEntity", "true"));
            String labels = cl.getOptionValue("labels", "");
            String groups = cl.getOptionValue("groups", "");
            
            // Validate required parameters
            if (username == null || password == null || application == null || 
                cluster == null || pov == null || dataFile == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c, -s, -f", 
                        "ExtractJournals", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Build journal extract options
            JournalExtractOptions options = new JournalExtractOptions();
            options.setDelimiter(delimiter);
            options.setUserFileName(dataFile);
            options.setExtractJournals(extractJournals);
            options.setExtractTemplates(extractTemplates);
            options.setPosted(posted);
            options.setApproved(approved);
            options.setSubmitted(submitted);
            options.setRejected(rejected);
            options.setWorking(working);
            options.setRegular(regular);
            options.setAutoreversing(autoreversing);
            options.setAutoreversal(autoreversal);
            options.setRecurringTemplates(recurringTemplates);
            options.setBalanced(balanced);
            options.setUnbalanced(unbalanced);
            options.setBalancedByEntity(balancedByEntity);
            
            if (labels != null && !labels.isEmpty()) {
                options.setLabels(labels);
            }
            if (groups != null && !groups.isEmpty()) {
                options.setGroups(groups);
            }
            
            List<String> povList = new ArrayList<>();
            povList.add(pov);
            
            // Execute extract
            LoadExtractOM extractOM = new LoadExtractOM(session.sessionInfo);
            List<Integer> taskIds = extractOM.extractJournals(povList, options);
            
            // Wait for completion
            boolean success = waitForTasks(session, taskIds, pollInterval, verbose);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            if (success) {
                jsonSuccess("Journals extract completed successfully", "ExtractJournals", 
                        application, elapsed, taskIds);
                return EXIT_SUCCESS;
            } else {
                jsonOutput(System.out, "Failed", "Journals extract task failed", 
                        "ExtractJournals", application, elapsed, taskIds);
                return EXIT_TASK_FAILED;
            }
            
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            jsonOutput(System.out, "Error", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "ExtractJournals", application, elapsed, null);
            if (verbose) e.printStackTrace(System.err);
            return EXIT_OPERATION_FAILED;
        } finally {
            if (session != null) session.close();
        }
    }

    // ==================== CLI Options Builder ====================
    
    private static Options buildOptions() {
        Options options = new Options();
        
        // Common options
        options.addOption("h", "help", false, "Show help message");
        options.addOption("v", "verbose", false, "Enable verbose output with progress updates");
        options.addOption("V", "version", false, "Show version information");
        
        // Authentication
        options.addOption("u", "user", true, "Username for HFM authentication");
        options.addOption("p", "password", true, "Password for HFM authentication");
        
        // Connection
        options.addOption("a", "app", true, "HFM application name");
        options.addOption("c", "cluster", true, "HFM cluster name");
        
        // Common parameters
        options.addOption("s", "pov", true, "Point of View (POV) string");
        options.addOption("f", "file", true, "Data file path");
        options.addOption("d", "delimiter", true, "File delimiter (default: ;)");
        options.addOption("t", "type", true, "Consolidation type: AllWithData, All, Impacted");
        
        // Polling
        options.addOption(Option.builder()
                .longOpt("pollInterval")
                .hasArg()
                .desc("Task polling interval in milliseconds (default: 2000)")
                .build());
        
        // Load Data options
        options.addOption(Option.builder().longOpt("loadMode").hasArg()
                .desc("Load mode: Merge, Replace, Accumulate").build());
        options.addOption(Option.builder().longOpt("accumulate").hasArg()
                .desc("Accumulate within file: true/false").build());
        options.addOption(Option.builder().longOpt("scanOnly").hasArg()
                .desc("Scan only mode: true/false").build());
        
        // Translate options
        options.addOption(Option.builder().longOpt("force").hasArg()
                .desc("Force translation: true/false").build());
        
        // Extract Data options
        options.addOption(Option.builder().longOpt("extractFormat").hasArg()
                .desc("Extract format: 'Flatfile - without header', 'Flatfile - with header'").build());
        options.addOption(Option.builder().longOpt("lineItemOption").hasArg()
                .desc("Line item option: None, Description, Both").build());
        options.addOption(Option.builder().longOpt("calculatedData").hasArg()
                .desc("Include calculated data: true/false").build());
        options.addOption(Option.builder().longOpt("derivedData").hasArg()
                .desc("Include derived data: true/false").build());
        options.addOption(Option.builder().longOpt("dynamicAccounts").hasArg()
                .desc("Include dynamic accounts: true/false").build());
        options.addOption(Option.builder().longOpt("prefix").hasArg()
                .desc("Table prefix for database extract").build());
        options.addOption(Option.builder().longOpt("dsn").hasArg()
                .desc("DSN for database extract").build());
        options.addOption(Option.builder().longOpt("schemaAction").hasArg()
                .desc("Schema action: 'Create Star Schema'").build());
        
        // Metadata options
        options.addOption(Option.builder().longOpt("fileFormat").hasArg()
                .desc("Metadata file format: app, xml").build());
        options.addOption(Option.builder().longOpt("accounts").hasArg()
                .desc("Extract accounts: true/false").build());
        options.addOption(Option.builder().longOpt("entities").hasArg()
                .desc("Extract entities: true/false").build());
        options.addOption(Option.builder().longOpt("scenarios").hasArg()
                .desc("Extract scenarios: true/false").build());
        options.addOption(Option.builder().longOpt("ICPs").hasArg()
                .desc("Extract ICPs: true/false").build());
        options.addOption(Option.builder().longOpt("currencies").hasArg()
                .desc("Extract currencies: true/false").build());
        options.addOption(Option.builder().longOpt("values").hasArg()
                .desc("Extract values: true/false").build());
        options.addOption(Option.builder().longOpt("applicationSettings").hasArg()
                .desc("Extract application settings: true/false").build());
        options.addOption(Option.builder().longOpt("consolidationMethods").hasArg()
                .desc("Extract consolidation methods: true/false").build());
        options.addOption(Option.builder().longOpt("cellTextLabels").hasArg()
                .desc("Extract cell text labels: true/false").build());
        options.addOption(Option.builder().longOpt("systemAccounts").hasArg()
                .desc("Extract system accounts: true/false").build());
        options.addOption(Option.builder().longOpt("customDimensions").hasArg()
                .desc("Custom dimensions to extract (semicolon-separated)").build());
        
        // Security options
        options.addOption(Option.builder().longOpt("securityClasses").hasArg()
                .desc("Extract security classes: true/false").build());
        options.addOption(Option.builder().longOpt("roleAccess").hasArg()
                .desc("Extract role access: true/false").build());
        options.addOption(Option.builder().longOpt("securityClassAccess").hasArg()
                .desc("Extract security class access: true/false").build());
        options.addOption(Option.builder().longOpt("loadUsers").hasArg()
                .desc("Extract users: true/false").build());
        
        // Journal options
        options.addOption(Option.builder().longOpt("extractJournals").hasArg()
                .desc("Extract journals: true/false").build());
        options.addOption(Option.builder().longOpt("extractTemplates").hasArg()
                .desc("Extract templates: true/false").build());
        options.addOption(Option.builder().longOpt("posted").hasArg()
                .desc("Include posted journals: true/false").build());
        options.addOption(Option.builder().longOpt("approved").hasArg()
                .desc("Include approved journals: true/false").build());
        options.addOption(Option.builder().longOpt("submitted").hasArg()
                .desc("Include submitted journals: true/false").build());
        options.addOption(Option.builder().longOpt("rejected").hasArg()
                .desc("Include rejected journals: true/false").build());
        options.addOption(Option.builder().longOpt("working").hasArg()
                .desc("Include working journals: true/false").build());
        options.addOption(Option.builder().longOpt("regular").hasArg()
                .desc("Include regular journals: true/false").build());
        options.addOption(Option.builder().longOpt("autoreversing").hasArg()
                .desc("Include autoreversing journals: true/false").build());
        options.addOption(Option.builder().longOpt("autoreversal").hasArg()
                .desc("Include autoreversal journals: true/false").build());
        options.addOption(Option.builder().longOpt("recurringTemplates").hasArg()
                .desc("Include recurring templates: true/false").build());
        options.addOption(Option.builder().longOpt("balanced").hasArg()
                .desc("Include balanced journals: true/false").build());
        options.addOption(Option.builder().longOpt("unbalanced").hasArg()
                .desc("Include unbalanced journals: true/false").build());
        options.addOption(Option.builder().longOpt("balancedByEntity").hasArg()
                .desc("Include balanced by entity journals: true/false").build());
        options.addOption(Option.builder().longOpt("labels").hasArg()
                .desc("Journal labels filter").build());
        options.addOption(Option.builder().longOpt("groups").hasArg()
                .desc("Journal groups filter").build());
        
        return options;
    }

    // ==================== Help Display ====================
    
    private static void printHelp(Options options) {
        System.out.println("HFM Command Line Interface v" + VERSION);
        System.out.println("Usage: java project1.HfmCli <operation> [options]");
        System.out.println();
        System.out.println("Operations:");
        System.out.println("  Consolidate            Run consolidation");
        System.out.println("  LoadData               Load data from file");
        System.out.println("  Translate              Run translation");
        System.out.println("  ExtractDataToFlatfile  Extract data to flat file");
        System.out.println("  ExtractDataToDatabase  Extract data to database");
        System.out.println("  ExtractMetadata        Extract metadata");
        System.out.println("  ExtractRules           Extract rules");
        System.out.println("  ExtractMemberLists     Extract member lists");
        System.out.println("  ExtractSecurity        Extract security");
        System.out.println("  ExtractJournals        Extract journals");
        System.out.println();
        
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(100);
        formatter.printHelp("HfmCli <operation>", options);
        
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  Consolidate:");
        System.out.println("    java project1.HfmCli Consolidate -u admin -p pass -a HCHFM -c HCHFMP \\");
        System.out.println("      -s \"S#Actual.Y#2025.P#Jan.E#CO_J00000...\" -t AllWithData");
        System.out.println();
        System.out.println("  Load Data:");
        System.out.println("    java project1.HfmCli LoadData -u admin -p pass -a HCHFM -c HCHFMP \\");
        System.out.println("      -f \"C:\\data\\load.dat\" -d \";\" --loadMode Merge");
        System.out.println();
        System.out.println("  Extract Data:");
        System.out.println("    java project1.HfmCli ExtractDataToFlatfile -u admin -p pass -a HCHFM -c HCHFMP \\");
        System.out.println("      -s \"S#Actual.Y#2025...\" -f \"C:\\extract\\data.txt\" --extractFormat \"Flatfile - without header\"");
        System.out.println();
        System.out.println("Exit Codes:");
        System.out.println("  0 - Success");
        System.out.println("  1 - Invalid arguments");
        System.out.println("  2 - Authentication failed");
        System.out.println("  3 - Operation failed");
        System.out.println("  4 - Task failed");
        System.out.println("  5 - Unknown error");
    }

    // ==================== Main Entry Point ====================
    
    public static void main(String[] args) {
        Options options = buildOptions();
        CommandLineParser parser = new DefaultParser();
        
        try {
            // Check for help or version first
            if (args.length == 0 || args[0].equals("-h") || args[0].equals("--help")) {
                printHelp(options);
                System.exit(EXIT_SUCCESS);
            }
            
            if (args[0].equals("-V") || args[0].equals("--version")) {
                System.out.println("{\"version\":\"" + VERSION + "\"}");
                System.exit(EXIT_SUCCESS);
            }
            
            // First argument is the operation
            String operation = args[0];
            
            // Parse remaining arguments
            String[] remainingArgs = new String[args.length - 1];
            System.arraycopy(args, 1, remainingArgs, 0, args.length - 1);
            
            CommandLine cl = parser.parse(options, remainingArgs);
            
            // Get common options
            boolean verbose = cl.hasOption("v");
            int pollInterval = Integer.parseInt(cl.getOptionValue("pollInterval", 
                    String.valueOf(DEFAULT_POLL_INTERVAL)));
            
            // Route to appropriate operation handler
            int exitCode;
            switch (operation.toLowerCase().replace("_", "")) {
                case "consolidate":
                    exitCode = doConsolidate(cl, pollInterval, verbose);
                    break;
                case "loaddata":
                case "load":
                    exitCode = doLoadData(cl, pollInterval, verbose);
                    break;
                case "translate":
                    exitCode = doTranslate(cl, pollInterval, verbose);
                    break;
                case "extractdatatoflatfile":
                case "extractdata":
                    exitCode = doExtractDataToFlatfile(cl, pollInterval, verbose);
                    break;
                case "extractdatatodatabase":
                case "extractdb":
                    exitCode = doExtractDataToDatabase(cl, pollInterval, verbose);
                    break;
                case "extractmetadata":
                case "metadata":
                    exitCode = doExtractMetadata(cl, pollInterval, verbose);
                    break;
                case "extractrules":
                case "rules":
                    exitCode = doExtractRules(cl, pollInterval, verbose);
                    break;
                case "extractmemberlists":
                case "memberlists":
                    exitCode = doExtractMemberLists(cl, pollInterval, verbose);
                    break;
                case "extractsecurity":
                case "security":
                    exitCode = doExtractSecurity(cl, pollInterval, verbose);
                    break;
                case "extractjournals":
                case "journals":
                    exitCode = doExtractJournals(cl, pollInterval, verbose);
                    break;
                default:
                    jsonError("Unknown operation: " + operation, operation, null);
                    exitCode = EXIT_INVALID_ARGS;
            }
            
            System.exit(exitCode);
            
        } catch (ParseException e) {
            jsonError("Failed to parse arguments: " + e.getMessage(), "Unknown", null);
            System.exit(EXIT_INVALID_ARGS);
        } catch (Exception e) {
            jsonError("Unexpected error: " + e.getMessage(), "Unknown", null);
            e.printStackTrace(System.err);
            System.exit(EXIT_UNKNOWN_ERROR);
        }
    }
}
