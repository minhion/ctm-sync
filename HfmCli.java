package com.bmc.ctm.hfmcli;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

public final class HfmCli {

    // ---------- utils ----------
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
        return "{\"status\":\"Error\",\"message\":\"" + (m == null ? "" : m.replace("\"","'")) + "\"}";
    }

    private static Map<String,Object> argMap(String[] argv) {
        Map<String,Object> out = new LinkedHashMap<String,Object>();
        String k = null;
        for (String a : argv) {
            if (a.startsWith("--")) {
                k = a.substring(2);
                out.put(k, Boolean.TRUE);            // presence-only default
            } else if (k != null) {
                out.put(k, a);
                k = null;
            }
        }
        return out;
    }

    private static String asString(Object v) {
        return (v == null || v instanceof Boolean) ? null : v.toString();
    }

    // ---------- login ----------
    /**
     * Attempt to obtain a SessionInfo by reflecting common HFM security APIs.
     * Returns the SessionInfo object (any type), or null if not available.
     */
    private static Object tryLogin(String user, String pass, String cluster, String provider, String domain, String server) {
        if (user == null || pass == null) return null;

        String[] factoryNames = new String[] {
            "oracle.epm.fm.common.service.ServiceClientFactory",
            "oracle.epm.fm.common.service.client.ServiceClientFactory"
        };
        String[] secGetters = new String[] { "getSecurityService", "getSecurityClient", "security", "getSecurity" };
        String[] loginNames = new String[] { "login", "authenticate", "logon" };

        for (int i = 0; i < factoryNames.length; i++) {
            String facName = factoryNames[i];
            try {
                Class<?> facClz = clz(facName);
                Object factory = null;
                try {
                    factory = callStatic(facClz, "getInstance", new Class<?>[] { });
                } catch (Throwable ign) {
                    try { factory = newInstance(facClz); } catch (Throwable t2) { factory = null; }
                }
                if (factory == null) continue;

                // Try to set connection hints on the factory
                if (provider != null) {
                    try { call(factory, "setProvider",    new Class<?>[]{String.class}, provider); } catch (Throwable ign) {}
                    try { call(factory, "setProviderURL", new Class<?>[]{String.class}, provider); } catch (Throwable ign) {}
                    try { call(factory, "setProviderUrl", new Class<?>[]{String.class}, provider); } catch (Throwable ign) {}
                }
                if (domain != null) {
                    try { call(factory, "setDomain",     new Class<?>[]{String.class}, domain); } catch (Throwable ign) {}
                    try { call(factory, "setDomainName", new Class<?>[]{String.class}, domain); } catch (Throwable ign) {}
                }
                if (server != null) {
                    try { call(factory, "setServer",     new Class<?>[]{String.class}, server); } catch (Throwable ign) {}
                    try { call(factory, "setServerName", new Class<?>[]{String.class}, server); } catch (Throwable ign) {}
                }
                if (cluster != null) {
                    try { call(factory, "setCluster",     new Class<?>[]{String.class}, cluster); } catch (Throwable ign) {}
                    try { call(factory, "setClusterName", new Class<?>[]{String.class}, cluster); } catch (Throwable ign) {}
                }

                Object sec = null;
                for (int g = 0; g < secGetters.length && sec == null; g++) {
                    try { sec = call(factory, secGetters[g], new Class<?>[] { }); } catch (Throwable ign) {}
                }
                if (sec == null) continue;

                // Set hints on the security client as well
                if (provider != null) {
                    try { call(sec, "setProvider",    new Class<?>[]{String.class}, provider); } catch (Throwable ign) {}
                    try { call(sec, "setProviderURL", new Class<?>[]{String.class}, provider); } catch (Throwable ign) {}
                    try { call(sec, "setProviderUrl", new Class<?>[]{String.class}, provider); } catch (Throwable ign) {}
                }
                if (domain != null) {
                    try { call(sec, "setDomain",     new Class<?>[]{String.class}, domain); } catch (Throwable ign) {}
                    try { call(sec, "setDomainName", new Class<?>[]{String.class}, domain); } catch (Throwable ign) {}
                }
                if (server != null) {
                    try { call(sec, "setServer",     new Class<?>[]{String.class}, server); } catch (Throwable ign) {}
                    try { call(sec, "setServerName", new Class<?>[]{String.class}, server); } catch (Throwable ign) {}
                }
                if (cluster != null) {
                    try { call(sec, "setCluster",     new Class<?>[]{String.class}, cluster); } catch (Throwable ign) {}
                    try { call(sec, "setClusterName", new Class<?>[]{String.class}, cluster); } catch (Throwable ign) {}
                }

                // Try common login signatures
                for (int m = 0; m < loginNames.length; m++) {
                    try {
                        return call(sec, loginNames[m],
                                    new Class<?>[] { String.class, String.class, String.class },
                                    user, pass, cluster);
                    } catch (NoSuchMethodException ns1) {
                        // next signature
                    } catch (Throwable ok3) {
                        return ok3; // some builds return a SessionInfo or throwable
                    }
                    try {
                        return call(sec, loginNames[m],
                                    new Class<?>[] { String.class, String.class },
                                    user, pass);
                    } catch (NoSuchMethodException ns2) {
                        // next signature
                    } catch (Throwable ok2) {
                        return ok2;
                    }
                    try {
                        return call(sec, loginNames[m],
                                    new Class<?>[] { String.class, String.class, String.class },
                                    user, pass, domain);
                    } catch (NoSuchMethodException ns3) {
                        // next method
                    } catch (Throwable ok1) {
                        return ok1;
                    }
                }
            } catch (Throwable ignoreAll) {
                // try next factory
            }
        }
        return null;
    }

    // ---------- main ----------
    public static void main(String[] args) {
        long t0 = System.currentTimeMillis();
        Map<String,Object> a = argMap(args);

        String op   = (args.length > 0) ? args[0] : "Consolidate";
        String app  = asString(a.get("application"));
        String type = asString(a.get("consolidationType"));
        if (type == null || type.isEmpty()) type = "AllWithData";
        String pov  = asString(a.get("pov"));

        String user    = System.getProperty("HFM_USER",     asString(a.get("user")));
        String pass    = System.getProperty("HFM_PASSWORD", asString(a.get("password")));
        String cluster = asString(a.get("cluster"));
        String provider= asString(a.get("provider"));
        String domain  = asString(a.get("domain"));
        String server  = asString(a.get("server"));

        boolean dryRun = Boolean.parseBoolean(String.valueOf(a.getOrDefault("dryRun","false")));

        try {
            if (!"Consolidate".equalsIgnoreCase(op)) {
                System.out.println(jsonErr("Only 'Consolidate' operation is supported"));
                System.exit(2);
            }
            if (app == null || pov == null) {
                System.out.println(jsonErr("Missing --application and/or --pov"));
                System.exit(2);
            }

            // Build parameter map with synonyms
            Map<String,Object> params = new LinkedHashMap<String,Object>();
            params.put("Application", app);
            params.put("ApplicationName", app);
            params.put("POV", pov);
            params.put("ConsolidationType", type);
            params.put("Type", type);

            if (cluster != null) { params.put("Cluster", cluster); params.put("ClusterName", cluster); }
            if (provider!= null) { params.put("Provider", provider); params.put("ProviderURL", provider); params.put("ProviderUrl", provider); }
            if (domain  != null) { params.put("Domain", domain); params.put("DomainName", domain); }
            if (server  != null) { params.put("Server", server); params.put("ServerName", server); }
            if (user    != null) { params.put("User", user); params.put("Username", user); }
            if (pass    != null) { params.put("Password", pass); }

            // Try to get SessionInfo
            Object sessionInfo = tryLogin(user, pass, cluster, provider, domain, server);
            if (sessionInfo != null) {
                params.put("sessionInfo", sessionInfo);
                params.put("SessionInfo", sessionInfo);
            }

            if (dryRun) {
                System.out.println("{\"status\":\"OK\",\"message\":\"DRY RUN\",\"application\":\""+app+"\"}");
                System.exit(0);
            }

            // Execute ConsolidateAction
            Class<?> actClz = clz("oracle.epm.fm.actions.ConsolidateAction");
            Object action = newInstance(actClz);

            Method exec = null;
            Method[] ms = actClz.getMethods();
            for (int i = 0; i < ms.length; i++) {
                if ("execute".equals(ms[i].getName()) && ms[i].getParameterCount() == 1) {
                    exec = ms[i]; break;
                }
            }
            if (exec == null) {
                System.out.println(jsonErr("Bind error: no execute(Map) on ConsolidateAction"));
                System.exit(5);
            }

            Object ok = exec.invoke(action, params);
            boolean success = !(ok instanceof Boolean) || ((Boolean) ok).booleanValue();

            long msElapsed = System.currentTimeMillis() - t0;
            if (success) {
                System.out.println("{\"status\":\"OK\",\"application\":\""+app+"\",\"elapsed_ms\":"+msElapsed+"}");
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
