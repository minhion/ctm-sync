package com.bmc.ctm.hfmcli;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal CLI for Oracle HFM "Consolidate" via fm-actions.jar.
 * - Parses args like: Consolidate --application APP --consolidationType AllWithData --pov "..." --cluster CLUS --provider URL ...
 * - Creates a SessionInfo by reflecting common HFM Security APIs (if available).
 * - Populates action parameter map with multiple key synonyms (Application/ApplicationName, Provider/ProviderURL, etc.).
 * - Calls oracle.epm.fm.actions.ConsolidateAction#execute(Map) and prints JSON.
 *
 * Requires classpath to include:
 *   epm_hfm_web.jar, epm_j2se.jar, epm_thrift.jar,
 *   fm-web-objectmodel.jar, fmcommon.jar, fm-actions.jar, fm-adm-driver.jar
 *
 * Also ensure PATH contains HFM native DLL locations:
 *   %EPM_ORACLE_HOME%\common\bin; %EPM_ORACLE_HOME%\products\FinancialManagement\Server\bin
 *
 * System properties (preferred) supplied by CTM wrapper:
 *   -DHFM_USER=...  -DHFM_PASSWORD=...
 * Optional CLI flags (fallback): --user --password --cluster --provider --domain --server
 */
public final class HfmCli {

    // ----------- util -----------
    private static Class<?> clz(String name) throws ClassNotFoundException {
        return Class.forName(name);
    }
    private static Object newInstance(Class<?> c) throws Exception {
        try { return c.getDeclaredConstructor().newInstance(); }
        catch (NoSuchMethodException e) { return c.newInstance(); }
    }
    private static Object call(Object target, String name, Class<?>[] sig, Object... args) throws Exception {
        Method m = target.getClass().getMethod(name, sig);
        m.setAccessible(true);
        return m.invoke(target, args);
    }
    private static Object callStatic(Class<?> c, String name, Class<?>[] sig, Object... args) throws Exception {
        Method m = c.getMethod(name, sig);
        m.setAccessible(true);
        return m.invoke(null, args);
    }
    private static String jsonErr(String m) {
        return "{\"status\":\"Error\",\"message\":\"" + (m==null? "" : m.replace("\"","'")) + "\"}";
    }
    private static Map<String,Object> argMap(String[] argv) {
        Map<String,Object> out = new LinkedHashMap<>();
        String k = null;
        for (String a : argv) {
            if (a.startsWith("--")) {
                k = a.substring(2);
                out.put(k, Boolean.TRUE);   // presence-only flag default
            } else if (k != null) {
                out.put(k, a);
                k = null;
            }
        }
        return out;
    }
    private static String asString(Object v) {
        // Treat missing/presence-only booleans as null to avoid ClassCastException
        return (v == null || v instanceof Boolean) ? null : v.toString();
    }

    // ----------- session login -----------
    /**
     * Try to create a SessionInfo using common HFM security APIs.
     * Tries several factories/client methods; tolerates builds where some classes are absent.
     */
    private static Object tryLogin(String user, String pass, String cluster, String provider, String domain, String server) {
        // No creds → skip
        if (user == null || pass == null) return null;

        // Candidate factory and security client classes frequently seen across 11.1.2.x builds
        final String[] FACTORIES = {
            "oracle.epm.fm.common.service.ServiceClientFactory",
            "oracle.epm.fm.common.service.client.ServiceClientFactory"
        };
        final String[] SEC_GETTERS = {"getSecurityService", "getSecurityClient", "security", "getSecurity"};
        final String[] LOGIN_METHODS = {"login", "authenticate", "logon"};

        // Some builds require setting provider/domain/server on factory or security before login
        final String[][] SETTERS = {
            {"setProvider","java.lang.String",}, {"setProviderURL","java.lang.String",}, {"setProviderUrl","java.lang.String",},
            {"setDomain","java.lang.String",},   {"setDomainName","java.lang.String",},
            {"setServer","java.lang.String",},   {"setServerName","java.lang.String",},
            {"setCluster","java.lang.String",},  {"setClusterName","java.lang.String",}
        };

        for (String facName : FACTORIES) {
            try {
                Class<?> facClz = clz(facName);
                Object factory = newInstance(facClz);

                // Try common static "getInstance" if present
                try { factory = callStatic(facClz, "getInstance", new Class<?>[]{}); } catch (Throwable ignore) {}

                // Opportunistically set provider/domain/server/cluster on the factory
                if (provider != null) { try { call(factory, "setProvider",    new Class<?>[]{String.class}, provider); } catch (Throwable ignore) {}
                                        try { call(factory, "setProviderURL", new Class<?>[]{String.class}, provider); }
