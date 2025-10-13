package project1;

import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.epm.fm.common.datatype.transport.SessionInfo;
import oracle.epm.fm.common.datatype.transport.WEBOMDATAGRIDTASKMASKENUM;
import oracle.epm.fm.domainobject.application.SessionOM;
import oracle.epm.fm.domainobject.metadata.MetadataOM;
import oracle.epm.fm.hssservice.HSSUtilManager;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

/**
 * HFMpovInfo (reflection-safe)
 *
 * What it does:
 *  1) Logs in and prints Custom dimension count & labels (via application profile, using reflection).
 *  2) Lists task enums (like your --list-types) without needing to log in.
 *  3) Optional: --validate "<POV>" parses S#/Y#/P#/E#/V#/Vw#/A#/I#/C1..C12, checks required tokens, and warns
 *     if the POV references a custom index greater than what the app supports.
 *
 * NOTE: No compile-time dependency on WEBOMDIMENSIONTYPE (enum names differ across patch levels).
 */
public class HFMpovInfo {

    public static void main(String[] args) {
        String username="", password="", appName="", cluster="";
        String validatePov = null;

        SessionOM sessionOM = null;
        SessionInfo session = null;

        try {
            // ---------- CLI ----------
            Options opt = new Options();
            opt.addOption("h", "help", false, "Help");
            opt.addOption("u", "user", true, "Username");
            opt.addOption("p", "password", true, "Password");
            opt.addOption("a", "app", true, "HFM application");
            opt.addOption("c", "cluster", true, "HFM cluster");
            opt.addOption(null, "validate", true, "Validate a POV string (e.g., \"S#Actual.Y#2025.P#Aug;Sep.E#CO_J00000.C1#Foo\")");
            opt.addOption(null, "list-types", false, "List available WEBOMDATAGRIDTASKMASKENUM values and exit");

            CommandLine cl = new BasicParser().parse(opt, args);

            if (cl.hasOption('h')) {
                new HelpFormatter().printHelp("HFMpovInfo", opt);
                return;
            }

            // no-login helper
            if (cl.hasOption("list-types")) {
                System.out.println("WEBOMDATAGRIDTASKMASKENUM values:");
                for (WEBOMDATAGRIDTASKMASKENUM e : WEBOMDATAGRIDTASKMASKENUM.values()) System.out.println(" - " + e.name());
                return;
            }

            if (cl.hasOption('u')) username = cl.getOptionValue("u"); else { System.err.println("Error: -u required"); System.exit(1); }
            if (cl.hasOption('p')) password = cl.getOptionValue("p"); else { System.err.println("Error: -p required"); System.exit(1); }
            if (cl.hasOption('a')) appName  = cl.getOptionValue("a"); else { System.err.println("Error: -a required"); System.exit(1); }
            if (cl.hasOption('c')) cluster  = cl.getOptionValue("c"); else { System.err.println("Error: -c required"); System.exit(1); }
            if (cl.hasOption("validate"))   validatePov = cl.getOptionValue("validate");

            // ---------- Login ----------
            System.out.println("Logging in to application " + appName + " with user " + username);
            String sso = HSSUtilManager.getSecurityManager().authenticateUser(username, password);
            sessionOM = new SessionOM();
            session = sessionOM.createSession(sso, java.util.Locale.ENGLISH, cluster, appName);

            MetadataOM md = new MetadataOM(session);

            // ---------- Application profile via reflection ----------
            Integer customCount = null;
            List<String> customLabels = null;
            try {
                Object profile = tryCall(md, "getApplicationProfile");
                if (profile == null) profile = tryCall(md, "getApplicationInfo");

                if (profile != null) {
                    customCount = callInt(profile, "getNumberOfCustomDimensions");
                    if (customCount == null) customCount = callInt(profile, "getCustomDimensionCount");
                    if (customCount == null) customCount = callInt(profile, "getCustomDimensionsCount");

                    // labels if available
                    @SuppressWarnings("unchecked")
                    List<String> names = (List<String>) tryCall(profile, "getCustomDimensionNames");
                    if (names == null && customCount != null) {
                        names = new ArrayList<String>();
                        for (int i = 1; i <= customCount; i++) {
                            String n = callString(profile, "getCustom" + i + "Name");
                            names.add(n != null ? n : "C" + i);
                        }
                    }
                    customLabels = names;
                }
            } catch (Throwable ignore) {
                // If all profile methods are missing, we just won’t show labels.
            }

            // If profile didn’t reveal a count, fall back to 0 (unknown) and just do POV shape checks.
            if (customCount == null) customCount = 0;

            System.out.println();
            System.out.println("===== POV / Dimension Info =====");
            System.out.println("Custom dimensions detected (via profile): " + customCount);
            if (customLabels != null && !customLabels.isEmpty()) {
                for (int i = 0; i < customLabels.size(); i++) {
                    System.out.println("  C" + (i+1) + " label: " + customLabels.get(i));
                }
            } else if (customCount > 0) {
                for (int i = 1; i <= customCount; i++) System.out.println("  C" + i + " label: (not available via API)");
            } else {
                System.out.println("  (Profile did not expose custom labels/count on this patch level.)");
            }

            // ---------- Optional: validate POV ----------
            if (validatePov != null) {
                System.out.println();
                System.out.println("===== Validating POV =====");
                System.out.println("POV: " + validatePov);

                PovTokens tokens = parsePov(validatePov);

                List<String> missing = new ArrayList<String>();
                if (tokens.scenario == null) missing.add("S#<Scenario>");
                if (tokens.year == null)     missing.add("Y#<Year>");
                if (tokens.periods.isEmpty()) missing.add("P#<Period(s)>");
                if (tokens.entity == null)   missing.add("E#<Entity>");

                if (!missing.isEmpty()) {
                    System.out.println("ERROR: Missing required tokens: " + missing);
                } else {
                    System.out.println("Required tokens present: S, Y, P, E");
                }

                int highestCustom = tokens.highestCustomIndex();
                if (customCount > 0 && highestCustom > customCount) {
                    System.out.println("ERROR: POV references C" + highestCustom + " but application has only " + customCount + " custom dimensions.");
                } else if (highestCustom > 0) {
                    for (int i = 1; i <= highestCustom; i++) {
                        if (!tokens.customs.containsKey(i)) {
                            System.out.println("WARNING: POV skipped C" + i + " (gaps can cause invalid intersections).");
                        }
                    }
                }

                tokens.prettyPrint();
            }

            if (sessionOM != null && session != null) sessionOM.closeSession(session);

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            try { if (sessionOM != null && session != null) sessionOM.closeSession(session); } catch (Exception ignore) {}
            System.exit(2);
        }
    }

    // ---------- reflection helpers ----------
    private static Object tryCall(Object target, String method) {
        try {
            Method m = target.getClass().getMethod(method);
            return m.invoke(target);
        } catch (Throwable t) { return null; }
    }
    private static Integer callInt(Object target, String method) {
        try {
            Method m = target.getClass().getMethod(method);
            Object v = m.invoke(target);
            return (v instanceof Integer) ? (Integer) v : null;
        } catch (Throwable t) { return null; }
    }
    private static String callString(Object target, String method) {
        try {
            Method m = target.getClass().getMethod(method);
            Object v = m.invoke(target);
            return (v instanceof String) ? (String) v : null;
        } catch (Throwable t) { return null; }
    }

    // ---------- POV parser ----------
    private static final Pattern TOKEN =
        Pattern.compile("(?i)\\b(S|Y|P|E|V|Vw|A|I|C([1-9]|1[0-2]))#([^\\.]+)");

    private static class PovTokens {
        String scenario, year, entity, value, view, account, icp;
        List<String> periods = new ArrayList<String>();
        Map<Integer, String> customs = new HashMap<Integer, String>();

        int highestCustomIndex() {
            int max = 0;
            for (Integer k : customs.keySet()) if (k != null && k > max) max = k;
            return max;
        }
        void prettyPrint() {
            System.out.println("Parsed POV tokens:");
            if (scenario != null) System.out.println("  S# " + scenario);
            if (year != null)     System.out.println("  Y# " + year);
            if (!periods.isEmpty()) System.out.println("  P# " + String.join(";", periods));
            if (entity != null)   Sys
