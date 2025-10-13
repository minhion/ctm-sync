package project1;

import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.epm.fm.common.datatype.transport.SessionInfo;
import oracle.epm.fm.common.datatype.transport.WEBOMDATAGRIDTASKMASKENUM;
import oracle.epm.fm.common.datatype.transport.WEBOMDIMENSIONTYPE;
import oracle.epm.fm.domainobject.application.SessionOM;
import oracle.epm.fm.domainobject.metadata.MetadataOM;
import oracle.epm.fm.hssservice.HSSUtilManager;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

/**
 * HFMpovInfo
 *
 * What it does:
 *  1) Logs in and prints Custom dimension count & labels (when available).
 *  2) Lists visible dimension enums & task enums (like your --list-types).
 *  3) Optional: --validate "<POV>" parses a POV like S#Actual.Y#2025.P#Aug;Sep.E#Entity.[Vw#YTD].[V#USD].C1#...
 *     and checks basic shape + whether the referenced Custom dimensions exist (C1..Ck).
 *
 * Notes:
 *  - Labels come from the application profile via reflection (method names differ across 11.1.2.x).
 *    If reflection fails, we still detect the Custom count by probing C1..C12 using MetadataOM.
 *  - This does *not* execute any server task; it’s safe to run in production.
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
            opt.addOption(null, "list-dims", false, "List available WEBOMDIMENSIONTYPE values and exit");
            opt.addOption(null, "list-types", false, "List available WEBOMDATAGRIDTASKMASKENUM values and exit");

            CommandLine cl = new BasicParser().parse(opt, args);

            if (cl.hasOption('h')) {
                new HelpFormatter().printHelp("HFMpovInfo", opt);
                return;
            }

            // quick local listings that require no login
            if (cl.hasOption("list-dims")) {
                System.out.println("WEBOMDIMENSIONTYPE values:");
                for (WEBOMDIMENSIONTYPE e : WEBOMDIMENSIONTYPE.values()) System.out.println(" - " + e.name());
                return;
            }
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
            session = sessionOM.createSession(sso, Locale.ENGLISH, cluster, appName);

            // ---------- Metadata ----------
            MetadataOM md = new MetadataOM(session);

            // Try to read application profile via reflection (method names differ by patch level)
            Integer customCount = null;
            List<String> customLabels = null;
            try {
                Object profile = tryCall(md, "getApplicationProfile");
                if (profile == null) profile = tryCall(md, "getApplicationInfo");
                if (profile != null) {
                    customCount = (Integer) tryCall(profile, "getNumberOfCustomDimensions");
                    if (customCount == null) customCount = (Integer) tryCall(profile, "getCustomDimensionCount");
                    if (customCount == null) customCount = (Integer) tryCall(profile, "getCustomDimensionsCount");

                    // labels if available
                    @SuppressWarnings("unchecked")
                    List<String> names = (List<String>) tryCall(profile, "getCustomDimensionNames");
                    if (names == null && customCount != null) {
                        names = new ArrayList<String>();
                        for (int i = 1; i <= customCount; i++) {
                            String n = (String) tryCall(profile, "getCustom" + i + "Name");
                            names.add(n != null ? n : "C" + i);
                        }
                    }
                    customLabels = names;
                }
            } catch (Throwable ignore) { /* fall back to probing */ }

            // If profile route failed, probe C1..C12 existance
            if (customCount == null) {
                int max = 0;
                for (int i = 1; i <= 12; i++) {
                    try {
                        WEBOMDIMENSIONTYPE dim = WEBOMDIMENSIONTYPE.valueOf("WEBOM_DIMENSION_CUSTOM" + i);
                        // any metadata call that requires the dim to exist (lightweight)
                        md.getMembers(dim, null, false);
                        max = i;
                    } catch (Throwable t) {
                        break;
                    }
                }
                customCount = max;
            }

            System.out.println();
            System.out.println("===== POV / Dimension Info =====");
            System.out.println("Custom dimensions detected: " + customCount);
            if (customLabels != null && !customLabels.isEmpty()) {
                for (int i = 0; i < customLabels.size(); i++) {
                    System.out.println("  C" + (i+1) + " label: " + customLabels.get(i));
                }
            }

            // Optional: validate POV shape & custom range (cheap checks)
            if (validatePov != null) {
                System.out.println();
                System.out.println("===== Validating POV =====");
                System.out.println("POV: " + validatePov);

                PovTokens tokens = parsePov(validatePov);

                // Required pieces
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

                // Custom range & order
                int highestCustom = tokens.highestCustomIndex();
                if (highestCustom > customCount) {
                    System.out.println("ERROR: POV references C" + highestCustom + " but application has only " + customCount + " custom dimensions.");
                } else if (highestCustom > 0) {
                    // ensure no gaps (e.g., C1 and C3 but no C2)
                    for (int i = 1; i <= highestCustom; i++) {
                        if (!tokens.customs.containsKey(i)) {
                            System.out.println("WARNING: POV skipped C" + i + " (gaps can cause invalid intersections).");
                        }
                    }
                }

                // Print what we parsed for quick eyeballing
                tokens.prettyPrint();
            }

            // Done
            if (sessionOM != null && session != null) sessionOM.closeSession(session);

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            try { if (sessionOM != null && session != null) sessionOM.closeSession(session); } catch (Exception ignore) {}
            System.exit(2);
        }
    }

    // --------- helpers ----------

    private static Object tryCall(Object target, String method) {
        try {
            Method m = target.getClass().getMethod(method);
            return m.invoke(target);
        } catch (Throwable t) { return null; }
    }

    /** POV tokenization **/
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
            if (entity != null)   System.out.println("  E# " + entity);
            if (view != null)     System.out.println("  Vw# " + view);
            if (value != null)    System.out.println("  V# " + value);
            if (account != null)  System.out.println("  A# " + account);
            if (icp != null)      System.out.println("  I# " + icp);
            if (!customs.isEmpty()) {
                List<Integer> keys = new ArrayList<Integer>(customs.keySet());
                Collections.sort(keys);
                for (int k : keys) System.out.println("  C" + k + "# " + customs.get(k));
            }
        }
    }

    private static PovTokens parsePov(String pov) {
        PovTokens t = new PovTokens();
        if (pov == null) return t;
        Matcher m = TOKEN.matcher(pov);
        while (m.find()) {
            String tag = m.group(1);
            String customIndexStr = m.group(2);
            String member = m.group(3);

            if ("S".equalsIgnoreCase(tag)) t.scenario = member;
            else if ("Y".equalsIgnoreCase(tag)) t.year = member;
            else if ("P".equalsIgnoreCase(tag)) t.periods = Arrays.asList(member.split(";"));
            else if ("E".equalsIgnoreCase(tag)) t.entity = member;
            else if ("Vw".equalsIgnoreCase(tag)) t.view = member;
            else if ("V".equalsIgnoreCase(tag)) t.value = member;
            else if ("A".equalsIgnoreCase(tag)) t.account = member;
            else if ("I".equalsIgnoreCase(tag)) t.icp = member;
            else if (customIndexStr != null) {
                int idx = Integer.parseInt(customIndexStr);
                t.customs.put(idx, member);
            }
        }
        return t;
    }
}
