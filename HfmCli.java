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
                                        try { call(factory, "setProviderURL", new Class<?>[]{String.class}, provider); } catch (Throwable ignore) {}
                                        try { call(factory, "setProviderUrl", new Class<?>[]{String.class}, provider); } catch (Throwable ignore) {} }
                if (domain   != null) { try { call(factory, "setDomain",      new Class<?>[]{String.class}, domain);   } catch (Throwable ignore) {}
                                        try { call(factory, "setDomainName",  new Class<?>[]{String.class}, domain);   } catch (Throwable ignore) {} }
                if (server   != null) { try { call(factory, "setServer",      new Class<?>[]{String.class}, server);   } catch (Throwable ignore) {}
                                        try { call(factory, "setServerName",  new Class<?>[]{String.class}, server);   } catch (Throwable ignore) {} }
                if (cluster  != null) { try { call(factory, "setCluster",     new Class<?>[]{String.class}, cluster);  } catch (Throwable ignore) {}
                                        try { call(factory, "setClusterName", new Class<?>[]{String.class}, cluster);  } catch (Throwable ignore) {} }

                // Obtain security client/service
                Object sec = null;
                for (String g : SEC_GETTERS) {
                    try { sec = call(factory, g, new Class<?>[]{}); } catch (Throwable ignore) {}
                    if (sec != null) break;
                }
                if (sec == null) continue;

                // Also try setting provider/domain/server/cluster on security object
                if (provider != null) { try { call(sec, "setProvider",    new Class<?>[]{String.class}, provider); } catch (Throwable ignore) {}
                                        try { call(sec, "setProviderURL", new Class<?>[]{String.class}, provider); } catch (Throwable ignore) {}
                                        try { call(sec, "setProviderUrl", new Class<?>[]{String.class}, provider); } catch (Throwable ignore) {} }
                if (domain   != null) { try { call(sec, "setDomain",      new Class<?>[]{String.class}, domain);   } catch (Throwable ignore) {}
                                        try { call(sec, "setDomainName",  new Class<?>[]{String.class}, domain);   } catch (Throwable ignore) {} }
                if (server   != null) { try { call(sec, "setServer",      new Class<?>[]{String.class}, server);   } catch (Throwable ignore) {}
                                        try { call(sec, "setServerName",  new Class<?>[]{String.class}, server);   } catch (Throwable ignore) {} }
                if (cluster  != null) { try { call(sec, "setCluster",     new Class<?>[]{String.class}, cluster);  } catch (Throwable ignore) {}
                                        try { call(sec, "setClusterName", new Class<?>[]{String.class}, cluster);  } catch (Throwable ignore) {} }

                // Try login/auth signatures
                for (String m : LOGIN_METHODS) {
                    try { // user, pass, cluster
                        return call(sec, m, new Class<?>[]{String.class,String.class,String.class}, user, pass, cluster);
                    } catch (NoSuchMethodException ignored) {}
                      catch (Throwable t) { return t; }

                    try { // user, pass
                        return call(sec, m, new Class<?>[]{String.class,String.class}, user, pass);
                    } catch (NoSuchMethodException ignored) {}
                      catch (Throwable t) { return t; }

                    try { // user, pass, domain
                        return call(sec, m, new Class<?>[]{String.class,String.class,String.class}, user, pass, domain);
                    } catch (NoSuchMethodException ignored) {}
                      catch (Throwable t) { return t; }
                }
            } catch (Throwable ignore) {
                // try the next factory name
            }
        }
        return null;
    }

    // ----------- main -----------
    public static void main(String[] args) {
        final long t0 = System.currentTimeMillis();
        final Map<String,Object> a = argMap(args);

        final String op   = (args.length > 0) ? args[0] : "Consolidate";
        String app        = asString(a.get("application"));
        String type       = asString(a.get("consolidationType"));
        if (type == null || type.isEmpty()) type = "AllWithData";
        String pov        = asString(a.get("pov"));

        // creds: prefer system props, fall back to flags
        final String user    = System.getProperty("HFM_USER",     asString(a.get("user")));
        final String pass    = System.getProperty("HFM_PASSWORD", asString(a.get("password")));
        final String cluster = asString(a.get("cluster"));
        final String provider= asString(a.get("provider"));     // e.g. http://HSS:28080/interop
        final String domain  = asString(a.get("domain"));       // AD or Native Directory
        final String server  = asString(a.get("server"));       // HFM App Server (optional)

        final boolean dryRun = Boolean.parseBoolean(String.valueOf(a.getOrDefault("dryRun", "false")));

        try {
            if (!"Consolidate".equalsIgnoreCase(op)) {
                System.out.println(jsonErr("Only 'Consolidate' operation is supported")); System.exit(2);
            }
            if (app == null || pov == null) {
                System.out.println(jsonErr("Missing --application and/or --pov")); System.exit(2);
            }

            // Build params map (include common synonyms so different builds can pick up)
            Map<String,Object> params = new LinkedHashMap<>();
            params.put("Application", app);
            params.put("ApplicationName", app);
            params.put("POV", pov);
            params.put("ConsolidationType", type);
            params.put("Type", type);

            if (cluster != null) {
                params.put("Cluster", cluster);
                params.put("ClusterName", cluster);
            }
            if (provider != null) {
                params.put("Provider", provider);
                params.put("ProviderURL", provider);
                params.put("ProviderUrl", provider);
            }
            if (domain != null) {
                params.put("Domain", domain);
                params.put("DomainName", domain);
            }
            if (server != null) {
                params.put("Server", server);
                params.put("ServerName", server);
            }
            if (user != null) {
                params.put("User", user);
                params.put("Username", user);
            }
            if (pass != null) {
                params.put("Password", pass);
            }

            // Try to obtain a SessionInfo (some builds require it explicitly)
            Object sessionInfo = tryLogin(user, pass, cluster, provider, domain, server);
            if (sessionInfo != null) {
                params.put("sessionInfo", sessionInfo);
                params.put("SessionInfo", sessionInfo);
            }

            if (dryRun) {
                System.out.println("{\"status\":\"OK\",\"message\":\"DRY RUN\",\"application\":\""+app+"\"}");
                System.exit(0);
            }

            // Call ConsolidateAction.execute(Map)
            Class<?> actClz = clz("oracle.epm.fm.actions.ConsolidateAction");
            Object action   = newInstance(actClz);

            Method exec = null;
            for (Method m : actClz.getMethods()) {
                if (m.getName().equals("execute") && m.getParameterCount() == 1) {
                    exec = m; break;
                }
            }
            if (exec == null) {
                System.out.println(jsonErr("Bind error: no execute(Map) on ConsolidateAction"));
                System.exit(5);
            }

            Object ok = exec.invoke(action, params);
            boolean success = !(ok instanceof Boolean) || (Boolean) ok;

            long ms = System.currentTimeMillis() - t0;
            if (success) {
                System.out.println("{\"status\":\"OK\",\"application\":\""+app+"\",\"elapsed_ms\":"+ms+"}");
                System.exit(0);
            } else {
                System.out.println(jsonErr("Consolidate returned false"));
                System.exit(6);
            }

        } catch (Throwable t) {
            t.printStackTrace(System.err);
            System.out.println(jsonErr(t.getClass().getSimpleName()+": "+t.getMessage()));
            System.exit(5);
        }
    }
}
