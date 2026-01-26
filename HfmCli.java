/*
 * HfmCli.java - HFM Command Line Interface for Control-M Integration
 * 
 * Compatible with Oracle EPM/HFM 11.1.2.0
 * Uses Apache Commons CLI 1.2 syntax (bundled with Oracle EPM)
 * 
 * Supported Operations:
 *   - Consolidate
 *   - Translate
 *   - LoadData
 *   - ExtractData
 *   - ExtractMetadata
 *   - ExtractRules
 *   - ExtractMemberLists
 *   - ExtractSecurity
 *   - ExtractJournals
 * 
 * Usage:
 *   java project1.HfmCli <operation> [options]
 */

package project1;

import java.io.File;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import oracle.epm.fm.common.datatype.transport.*;
import oracle.epm.fm.domainobject.administration.AdministrationOM;
import oracle.epm.fm.domainobject.application.SessionOM;
import oracle.epm.fm.domainobject.data.DataOM;
import oracle.epm.fm.domainobject.loadextract.LoadExtractOM;
import oracle.epm.fm.domainobject.loadextract.LoadExtractInfo;
import oracle.epm.fm.hssservice.HSSUtilManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class HfmCli {

    // ==================== Constants ====================
    private static final String VERSION = "2.2.0";
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
        
        if (taskIds == null || taskIds.isEmpty()) {
            return true; // No tasks to wait for
        }
        
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
                    anyFailed = true;
                }
            }
            
            if (!allCompleted) {
                Thread.sleep(pollInterval);
            }
        }
        
        return !anyFailed;
    }
    
    private static boolean waitForServerTask(HfmSession session, ServerTaskInfo taskInfo,
            int pollInterval, boolean verbose) throws Exception {
        List<Integer> taskIds = taskInfo.getTaskIDs();
        return waitForTasks(session, taskIds, pollInterval, verbose);
    }
    
    // Wait for single task ID (used by extractData which returns int)
    private static boolean waitForTask(HfmSession session, int taskId,
            int pollInterval, boolean verbose) throws Exception {
        List<Integer> taskIds = new ArrayList<Integer>();
        taskIds.add(taskId);
        return waitForTasks(session, taskIds, pollInterval, verbose);
    }

    // ==================== Consolidation Types Mapping ====================
    
    private static WEBOMDATAGRIDTASKMASKENUM getConsolidationType(String type) {
        if (type == null) type = "AllWithData";
        
        String normalized = type.toLowerCase().replace(" ", "").replace("_", "");
        if (normalized.equals("allwithdata")) {
            return WEBOMDATAGRIDTASKMASKENUM.WEBOM_DATAGRID_TASK_CONSOLIDATEALLWITHDATA;
        } else if (normalized.equals("all")) {
            return WEBOMDATAGRIDTASKMASKENUM.WEBOM_DATAGRID_TASK_CONSOLIDATEALL;
        } else if (normalized.equals("impacted")) {
            return WEBOMDATAGRIDTASKMASKENUM.WEBOM_DATAGRID_TASK_CONSOLIDATE;
        } else if (normalized.equals("forcecalculate") || normalized.equals("force")) {
            return WEBOMDATAGRIDTASKMASKENUM.WEBOM_DATAGRID_TASK_FORCECALCULATE;
        } else {
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
            List<String> povList = new ArrayList<String>();
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
            options.setAppendToLogFile(false);
            options.setContainSharesData(true);
            options.setContainSubmissionPhaseData(false);
            options.setDecimalChar("");
            options.setThousandsChar("");
            options.loadCalculated = false;
            
            // Set load mode
            if ("replace".equalsIgnoreCase(loadMode)) {
                options.setDuplicates(DATALOAD_DUPLICATE_HANDLING.DATALOAD_REPLACE);
            } else if ("accumulate".equalsIgnoreCase(loadMode)) {
                options.setDuplicates(DATALOAD_DUPLICATE_HANDLING.DATALOAD_ACCUMULATE);
            } else {
                options.setDuplicates(DATALOAD_DUPLICATE_HANDLING.DATALOAD_MERGE);
            }
            options.setMode(LOAD_MODE.LOAD);
            options.setFileFormat(DATALOAD_FILE_FORMAT.DATALOAD_FILE_FORMAT_NATIVE);
            
            List<DataLoadOptions> optsList = new ArrayList<DataLoadOptions>();
            optsList.add(options);
            
            List<String> files = new ArrayList<String>();
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
            List<String> povList = new ArrayList<String>();
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

    // ==================== Operation: Extract Data ====================
    
    private static int doExtractData(CommandLine cl, int pollInterval, boolean verbose) {
        long startTime = System.currentTimeMillis();
        String application = cl.getOptionValue("a");
        HfmSession session = null;
        
        try {
            String username = cl.getOptionValue("u");
            String password = cl.getOptionValue("p");
            String cluster = cl.getOptionValue("c");
            String pov = cl.getOptionValue("s");
            String delimiter = cl.getOptionValue("d", ";");
            String extractFormat = cl.getOptionValue("extractFormat", "flatfile");
            boolean calculatedData = Boolean.parseBoolean(cl.getOptionValue("calculatedData", "false"));
            boolean derivedData = Boolean.parseBoolean(cl.getOptionValue("derivedData", "false"));
            boolean dynamicAccounts = Boolean.parseBoolean(cl.getOptionValue("dynamicAccounts", "false"));
            
            // Database options
            String dsn = cl.getOptionValue("dsn");
            String prefix = cl.getOptionValue("prefix", "HFM_");
            
            // Validate required parameters
            if (username == null || password == null || application == null || 
                cluster == null || pov == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c, -s", "ExtractData", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Build extract options
            DataExtractOptions options = new DataExtractOptions();
            options.setDelimiter(delimiter);
            options.setMetadataSlice(pov);  // POV goes in metadataSlice
            options.setIncludeCalculatedData(calculatedData);
            options.setIncludeDerivedData(derivedData);
            options.setIncludeDynamicAccounts(dynamicAccounts);
            options.setIncludeData(true);
            
            // Set extract format based on type
            String fmt = extractFormat.toLowerCase().replace(" ", "").replace("_", "");
            if (fmt.contains("noheader") || fmt.contains("flatfilenoheader")) {
                options.setExtractFormat(DATA_EXTRACT_TYPE_FLAG.EA_EXTRACT_TYPE_FLATFILE_NOHEADER);
            } else if (fmt.contains("flatfile") || fmt.contains("standard")) {
                options.setExtractFormat(DATA_EXTRACT_TYPE_FLAG.EA_EXTRACT_TYPE_FLATFILE);
            } else if (fmt.contains("warehouse") || fmt.contains("database")) {
                options.setExtractFormat(DATA_EXTRACT_TYPE_FLAG.EA_EXTRACT_TYPE_WAREHOUSE);
                if (dsn != null) {
                    options.setDSN(dsn);
                    options.setTablePrefix(prefix);
                }
            } else if (fmt.contains("essbase")) {
                options.setExtractFormat(DATA_EXTRACT_TYPE_FLAG.EA_EXTRACT_TYPE_ESSBASE);
            } else {
                options.setExtractFormat(DATA_EXTRACT_TYPE_FLAG.EA_EXTRACT_TYPE_FLATFILE);
            }
            
            // Execute extract - returns single int task ID
            LoadExtractOM extractOM = new LoadExtractOM(session.sessionInfo);
            int taskId = extractOM.extractData(options);
            
            // Wait for completion
            List<Integer> taskIds = new ArrayList<Integer>();
            taskIds.add(taskId);
            boolean success = waitForTask(session, taskId, pollInterval, verbose);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            if (success) {
                jsonSuccess("Data extract completed successfully", "ExtractData", 
                        application, elapsed, taskIds);
                return EXIT_SUCCESS;
            } else {
                jsonOutput(System.out, "Failed", "Data extract task failed", 
                        "ExtractData", application, elapsed, taskIds);
                return EXIT_TASK_FAILED;
            }
            
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            jsonOutput(System.out, "Error", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "ExtractData", application, elapsed, null);
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
            String delimiter = cl.getOptionValue("d", ";");
            String fileFormat = cl.getOptionValue("fileFormat", "app");
            
            // Metadata options - default all to true
            boolean accounts = Boolean.parseBoolean(cl.getOptionValue("accounts", "true"));
            boolean entities = Boolean.parseBoolean(cl.getOptionValue("entities", "true"));
            boolean scenarios = Boolean.parseBoolean(cl.getOptionValue("scenarios", "true"));
            boolean currencies = Boolean.parseBoolean(cl.getOptionValue("currencies", "true"));
            boolean values = Boolean.parseBoolean(cl.getOptionValue("values", "true"));
            boolean icps = Boolean.parseBoolean(cl.getOptionValue("ICPs", "true"));
            boolean appSettings = Boolean.parseBoolean(cl.getOptionValue("appSettings", "true"));
            boolean consolMethods = Boolean.parseBoolean(cl.getOptionValue("consolMethods", "true"));
            boolean cellTxtLabels = Boolean.parseBoolean(cl.getOptionValue("cellTxtLabels", "true"));
            boolean systemAccounts = Boolean.parseBoolean(cl.getOptionValue("systemAccounts", "false"));
            
            // Validate required parameters
            if (username == null || password == null || application == null || cluster == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c", "ExtractMetadata", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Build extract options
            MetadataExtractOptions options = new MetadataExtractOptions();
            options.setDelimiter(delimiter);
            options.setAccounts(accounts);
            options.setEntities(entities);
            options.setScenarios(scenarios);
            options.setCurrencies(currencies);
            options.setValues(values);
            options.setICPs(icps);
            options.setAppSettings(appSettings);
            options.setConsolMethods(consolMethods);
            options.setCellTxtLabels(cellTxtLabels);
            options.setSystemAccounts(systemAccounts);
            options.setYears(true);
            options.setPeriods(true);
            options.setViews(true);
            
            // Set file format
            if ("xml".equalsIgnoreCase(fileFormat)) {
                options.setFileFormat(METADATA_FILE_FORMAT_ENUM.METADATA_FILE_FORMAT_ENUM_XML);
            } else {
                options.setFileFormat(METADATA_FILE_FORMAT_ENUM.METADATA_FILE_FORMAT_ENUM_APP);
            }
            
            // Execute extract - returns LoadExtractInfo
            LoadExtractOM extractOM = new LoadExtractOM(session.sessionInfo);
            LoadExtractInfo info = extractOM.extractMetadata(options);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            // LoadExtractInfo contains status - check if successful
            String statusMsg = info != null ? "Metadata extract completed" : "Metadata extract returned null";
            jsonSuccess(statusMsg, "ExtractMetadata", application, elapsed, null);
            return EXIT_SUCCESS;
            
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
            String fileFormat = cl.getOptionValue("fileFormat", "xml");
            
            // Validate required parameters
            if (username == null || password == null || application == null || cluster == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c", "ExtractRules", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Determine file format enum
            RULESEXTRACT_FILE_FORMAT format;
            if ("rle".equalsIgnoreCase(fileFormat)) {
                format = RULESEXTRACT_FILE_FORMAT.RULESEXTRACT_FILE_FORMAT_RLE;
            } else {
                format = RULESEXTRACT_FILE_FORMAT.RULESEXTRACT_FILE_FORMAT_XML;
            }
            
            // Execute extract - returns LoadExtractInfo
            LoadExtractOM extractOM = new LoadExtractOM(session.sessionInfo);
            LoadExtractInfo info = extractOM.extractRules(format);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            String statusMsg = info != null ? "Rules extract completed" : "Rules extract returned null";
            jsonSuccess(statusMsg, "ExtractRules", application, elapsed, null);
            return EXIT_SUCCESS;
            
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
            
            // Validate required parameters
            if (username == null || password == null || application == null || cluster == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c", "ExtractMemberLists", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Execute extract - takes no arguments, returns LoadExtractInfo
            LoadExtractOM extractOM = new LoadExtractOM(session.sessionInfo);
            LoadExtractInfo info = extractOM.extractMemberLists();
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            String statusMsg = info != null ? "Member lists extract completed" : "Member lists extract returned null";
            jsonSuccess(statusMsg, "ExtractMemberLists", application, elapsed, null);
            return EXIT_SUCCESS;
            
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
            String delimiter = cl.getOptionValue("d", ";");
            
            // Security options
            boolean users = Boolean.parseBoolean(cl.getOptionValue("users", "true"));
            boolean securityClasses = Boolean.parseBoolean(cl.getOptionValue("securityClasses", "true"));
            boolean roleAccess = Boolean.parseBoolean(cl.getOptionValue("roleAccess", "true"));
            boolean securityClassAccess = Boolean.parseBoolean(cl.getOptionValue("securityClassAccess", "true"));
            
            // Validate required parameters
            if (username == null || password == null || application == null || cluster == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c", "ExtractSecurity", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Build extract options
            SecurityExtractOptions options = new SecurityExtractOptions();
            options.setDelimiter(delimiter);
            options.setUsers(users);
            options.setSecurityClasses(securityClasses);
            options.setRoleAccess(roleAccess);
            options.setSecurityClassAccess(securityClassAccess);
            options.setFileFormat(SECURITYEXTRACT_FILEFORMAT.SECURITYEXTRACT_FILEFORMAT_NATIVE);
            
            // Execute extract - returns LoadExtractInfo
            LoadExtractOM extractOM = new LoadExtractOM(session.sessionInfo);
            LoadExtractInfo info = extractOM.extractSecurity(options);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            String statusMsg = info != null ? "Security extract completed" : "Security extract returned null";
            jsonSuccess(statusMsg, "ExtractSecurity", application, elapsed, null);
            return EXIT_SUCCESS;
            
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
            String delimiter = cl.getOptionValue("d", ";");
            
            // Journal options
            boolean regular = Boolean.parseBoolean(cl.getOptionValue("regular", "true"));
            boolean standard = Boolean.parseBoolean(cl.getOptionValue("standard", "true"));
            boolean recurring = Boolean.parseBoolean(cl.getOptionValue("recurring", "false"));
            String labelsStr = cl.getOptionValue("labels");
            String groupsStr = cl.getOptionValue("groups");
            
            // Validate required parameters
            if (username == null || password == null || application == null || 
                cluster == null || pov == null) {
                jsonError("Missing required parameters: -u, -p, -a, -c, -s", "ExtractJournals", application);
                return EXIT_INVALID_ARGS;
            }
            
            // Create session
            session = createSession(username, password, cluster, application);
            
            // Build extract options
            JournalExtractOptions options = new JournalExtractOptions();
            options.setDelimiter(delimiter);
            options.setPov(pov);
            options.setRegular(regular);
            options.setStandard(standard);
            options.setRecurring(recurring);
            
            // Set labels if provided
            if (labelsStr != null && !labelsStr.isEmpty()) {
                List<String> labels = Arrays.asList(labelsStr.split("[;,]"));
                options.setLabels(labels);
            }
            
            // Set groups if provided
            if (groupsStr != null && !groupsStr.isEmpty()) {
                List<String> groups = Arrays.asList(groupsStr.split("[;,]"));
                options.setGroups(groups);
            }
            
            // Execute extract - returns LoadExtractInfo
            LoadExtractOM extractOM = new LoadExtractOM(session.sessionInfo);
            LoadExtractInfo info = extractOM.extractJournals(options);
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            String statusMsg = info != null ? "Journals extract completed" : "Journals extract returned null";
            jsonSuccess(statusMsg, "ExtractJournals", application, elapsed, null);
            return EXIT_SUCCESS;
            
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

    // ==================== Command Line Options ====================
    
    @SuppressWarnings("static-access")
    private static Options buildOptions() {
        Options options = new Options();
        
        // Common options using Commons CLI 1.2 OptionBuilder syntax
        options.addOption(OptionBuilder
                .withLongOpt("user")
                .withDescription("HFM username")
                .hasArg()
                .withArgName("USERNAME")
                .create("u"));
        
        options.addOption(OptionBuilder
                .withLongOpt("password")
                .withDescription("HFM password")
                .hasArg()
                .withArgName("PASSWORD")
                .create("p"));
        
        options.addOption(OptionBuilder
                .withLongOpt("app")
                .withDescription("HFM application name")
                .hasArg()
                .withArgName("APP")
                .create("a"));
        
        options.addOption(OptionBuilder
                .withLongOpt("cluster")
                .withDescription("HFM cluster name")
                .hasArg()
                .withArgName("CLUSTER")
                .create("c"));
        
        options.addOption(OptionBuilder
                .withLongOpt("slice")
                .withDescription("POV slice string")
                .hasArg()
                .withArgName("POV")
                .create("s"));
        
        options.addOption(OptionBuilder
                .withLongOpt("type")
                .withDescription("Consolidation type: AllWithData, All, Impacted, ForceCalculate")
                .hasArg()
                .withArgName("TYPE")
                .create("t"));
        
        options.addOption(OptionBuilder
                .withLongOpt("file")
                .withDescription("Data file path")
                .hasArg()
                .withArgName("FILE")
                .create("f"));
        
        options.addOption(OptionBuilder
                .withLongOpt("delimiter")
                .withDescription("Field delimiter (default: ;)")
                .hasArg()
                .withArgName("DELIM")
                .create("d"));
        
        options.addOption(OptionBuilder
                .withLongOpt("verbose")
                .withDescription("Enable verbose output")
                .create("v"));
        
        options.addOption(OptionBuilder
                .withLongOpt("help")
                .withDescription("Show help")
                .create("h"));
        
        options.addOption(OptionBuilder
                .withLongOpt("version")
                .withDescription("Show version")
                .create("V"));
        
        // Load options
        options.addOption(OptionBuilder.withLongOpt("loadMode")
                .hasArg().withDescription("Load mode: Merge, Replace, Accumulate").create());
        options.addOption(OptionBuilder.withLongOpt("accumulate")
                .hasArg().withDescription("Accumulate within file: true/false").create());
        
        // Translate options
        options.addOption(OptionBuilder.withLongOpt("force")
                .hasArg().withDescription("Force translate: true/false").create());
        
        // Extract data options
        options.addOption(OptionBuilder.withLongOpt("extractFormat")
                .hasArg().withDescription("Extract format: flatfile, flatfileNoHeader, warehouse, essbase").create());
        options.addOption(OptionBuilder.withLongOpt("calculatedData")
                .hasArg().withDescription("Include calculated data: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("derivedData")
                .hasArg().withDescription("Include derived data: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("dynamicAccounts")
                .hasArg().withDescription("Include dynamic accounts: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("dsn")
                .hasArg().withDescription("Database DSN for warehouse extract").create());
        options.addOption(OptionBuilder.withLongOpt("prefix")
                .hasArg().withDescription("Table prefix for warehouse extract").create());
        
        // Metadata extract options
        options.addOption(OptionBuilder.withLongOpt("fileFormat")
                .hasArg().withDescription("File format: app, xml, rle").create());
        options.addOption(OptionBuilder.withLongOpt("accounts")
                .hasArg().withDescription("Extract accounts: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("entities")
                .hasArg().withDescription("Extract entities: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("scenarios")
                .hasArg().withDescription("Extract scenarios: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("currencies")
                .hasArg().withDescription("Extract currencies: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("values")
                .hasArg().withDescription("Extract values: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("ICPs")
                .hasArg().withDescription("Extract ICPs: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("appSettings")
                .hasArg().withDescription("Extract app settings: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("consolMethods")
                .hasArg().withDescription("Extract consolidation methods: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("cellTxtLabels")
                .hasArg().withDescription("Extract cell text labels: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("systemAccounts")
                .hasArg().withDescription("Extract system accounts: true/false").create());
        
        // Security extract options
        options.addOption(OptionBuilder.withLongOpt("users")
                .hasArg().withDescription("Extract users: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("securityClasses")
                .hasArg().withDescription("Extract security classes: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("roleAccess")
                .hasArg().withDescription("Extract role access: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("securityClassAccess")
                .hasArg().withDescription("Extract security class access: true/false").create());
        
        // Journal extract options
        options.addOption(OptionBuilder.withLongOpt("regular")
                .hasArg().withDescription("Include regular journals: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("standard")
                .hasArg().withDescription("Include standard journals: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("recurring")
                .hasArg().withDescription("Include recurring journals: true/false").create());
        options.addOption(OptionBuilder.withLongOpt("labels")
                .hasArg().withDescription("Journal labels filter (comma-separated)").create());
        options.addOption(OptionBuilder.withLongOpt("groups")
                .hasArg().withDescription("Journal groups filter (comma-separated)").create());
        
        // Poll interval
        options.addOption(OptionBuilder.withLongOpt("pollInterval")
                .hasArg().withDescription("Task polling interval in ms (default: 2000)").create());
        
        return options;
    }

    // ==================== Help Display ====================
    
    private static void printHelp(Options options) {
        System.out.println("HFM Command Line Interface v" + VERSION);
        System.out.println("Compatible with Oracle EPM/HFM 11.1.2.0");
        System.out.println();
        System.out.println("Usage: java project1.HfmCli <operation> [options]");
        System.out.println();
        System.out.println("Operations:");
        System.out.println("  Consolidate        Run consolidation on POV");
        System.out.println("  LoadData           Load data from file");
        System.out.println("  Translate          Run translation on POV");
        System.out.println("  ExtractData        Extract data to file/database");
        System.out.println("  ExtractMetadata    Extract metadata");
        System.out.println("  ExtractRules       Extract rules");
        System.out.println("  ExtractMemberLists Extract member lists");
        System.out.println("  ExtractSecurity    Extract security");
        System.out.println("  ExtractJournals    Extract journals");
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
        System.out.println("    java project1.HfmCli ExtractData -u admin -p pass -a HCHFM -c HCHFMP \\");
        System.out.println("      -s \"S#Actual.Y#2025...\" --extractFormat flatfile");
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
        CommandLineParser parser = new GnuParser();  // Commons CLI 1.2 parser
        
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
            int pollInterval = DEFAULT_POLL_INTERVAL;
            if (cl.hasOption("pollInterval")) {
                pollInterval = Integer.parseInt(cl.getOptionValue("pollInterval"));
            }
            
            // Route to appropriate operation handler
            int exitCode;
            String op = operation.toLowerCase().replace("_", "");
            
            if (op.equals("consolidate")) {
                exitCode = doConsolidate(cl, pollInterval, verbose);
            } else if (op.equals("loaddata") || op.equals("load")) {
                exitCode = doLoadData(cl, pollInterval, verbose);
            } else if (op.equals("translate")) {
                exitCode = doTranslate(cl, pollInterval, verbose);
            } else if (op.equals("extractdata") || op.equals("extract")) {
                exitCode = doExtractData(cl, pollInterval, verbose);
            } else if (op.equals("extractmetadata") || op.equals("metadata")) {
                exitCode = doExtractMetadata(cl, pollInterval, verbose);
            } else if (op.equals("extractrules") || op.equals("rules")) {
                exitCode = doExtractRules(cl, pollInterval, verbose);
            } else if (op.equals("extractmemberlists") || op.equals("memberlists")) {
                exitCode = doExtractMemberLists(cl, pollInterval, verbose);
            } else if (op.equals("extractsecurity") || op.equals("security")) {
                exitCode = doExtractSecurity(cl, pollInterval, verbose);
            } else if (op.equals("extractjournals") || op.equals("journals")) {
                exitCode = doExtractJournals(cl, pollInterval, verbose);
            } else {
                jsonError("Unknown operation: " + operation, operation, null);
                printHelp(options);
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
