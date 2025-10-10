package com.bmc.ctm.hfmcli;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

public final class HfmCli {

    // ---------- reflect helpers ----------
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
    private static boolean safeCall(Object tgt, String name, Class<?>[] sig, Object... args) {
        try { call(tgt, name, sig, args); return true; } catch (Throwable ignore) { return false; }
    }
    private static Object safeCallRet(Object tgt, String name, Class<?>[] sig, Object... args) {
        try { return call(tgt, name, sig, args); } catch (Throwable ignore) { return null; }
    }
    private static Object safeStaticRet(Class<?> c, String name, Class<?>[] sig, Object... args) {
        try { return callStatic(c, name, sig, args); } catch (Throwable ignore) { return null; }
    }

    // ---------- small utils ----------
    private static String jsonErr(String m) {
        return "{\"status\":\"Error\",\"message\":\"" + (m == null ? "" : m.replace("\"","'")) + "\"}";
    }
    private static Map<String,Object> argMap(String[] argv) {
        Map<String,Object> out = new LinkedHashMap<String,Object>();
        String k = null;
        for (String a : argv) {
            if (a.startsWith("--")) { k = a.substring(2); out.put(k, Boolean.TRUE); }
            else if (k != null) { out.put(k, a); k = null; }
        }
        return out;
    }
    private static String asString(Object v) {
        return (v == null || v instanceof Boolean) ? null : v.toString();
    }
    private static boolean isTrue(Object v) {
        if (v == null) return false;
        String s = String.valueOf(v).trim();
        return s.equalsIgnoreCase("true") || s.equalsIgnoreCase("yes") || s.equals("1");
    }

    // ---------- login ----------
    private static Object tryLogin(String user, String pass, String cluster, String provider, String domain, String server) {
        if (user == null || pass == null) return null;

        String[] factoryNames = {
            "oracle.epm.fm.common.service.ServiceClientFactory",
            "oracle.epm.fm.common.service.client.ServiceClientFactory"
        };
        String[] secGetters = { "getSecurityService", "getSecurityClient", "security", "getSecurity" };
        String[] loginNames = { "login", "authenticate", "logon" };

        for (String fname : factoryNames) {
            try {
                Class<?> facClz = clz(fname);

                Object factory = safeStaticRet(facClz, "getInstance", new Class<?>[] {});
                if (factory == null) {
                    try { factory = newInstance(facClz); } catch (Throwable ignore) { factory = null; }
                }
                if (factory == null) continue;

                // optional hints on factory
                if (provider != null) {
                    safeCall(factory, "setProvider",    new Class<?>[]{String.class}, provider);
                    safeCall(factory, "setProviderURL", new Class<?>[]{String.class}, provider);
                    safeCall(factory, "setProviderUrl", new Class<?>[]{String.class}, provider);
                }
                if (domain != null) {
                    safeCall(factory, "setDomain",     new Class<?>[]{String.class}, domain);
                    safeCall(factory, "setDomainName", new Class<?>[]{String.class}, domain);
                }
                if (server != null) {
                    safeCall(factory, "setServer",     new Class<?>[]{String.class}, server);
                    safeCall(factory, "setServerName", new Class<?>[]{String.class}, server);
                }
                if (cluster != null) {
                    safeCall(factory, "setCluster",     new Class<?>[]{String.class}, cluster);
                    safeCall(factory, "setClusterName", new Class<?>[]{String.class}, cluster);
                }

                Object sec = null;
                for (String g : secGetters) {
                    if (sec != null) break;
                    sec = safeCallRet(factory, g, new Class<?>[] {});
                }
                if (sec == null) continue;

                // optional hints on client
                if (provider != null) {
                    safeCall(sec, "setProvider",    new Class<?>[]{String.class}, provider);
                    safeCall(sec, "setProviderURL", new Class<?>[]{String.class}, provider);
                    safeCall(sec, "setProviderUrl", new Class<?>[]{String.class}, provider);
                }
                if (domain != null) {
                    safeCall(sec, "setDomain",     new Class<?>[]{String.class}, domain);
                    safeCall(sec, "setDomainName", new Class<?>[]{String.class}, domain);
                }
                if (server != null) {
                    safeCall(sec, "setServer",     new Class<?>[]{String.class}, server);
                    safeCall(sec, "setServerName", new Class<?>[]{String.class}, server);
                }
                if (cluster != null) {
                    safeCall(sec, "setCluster",     new Class<?>[]{String.class}, cluster);
                    safeCall(sec, "setClusterName", new Class<?>[]{String.class}, cluster);
                }

                // attempt common signatures
                for (String ln : loginNames) {
                    Object r = safeCallRet(sec, ln,
                            new Class<?>[]{String.class,String.class,String.class}, user, pass, cluster);
                    if (r != null) return r;

                    r = safeCallRet(sec, ln, new Class<?>[]{String.class,String.class}, user, pass);
                    if (r != null) return r;

                    r = safeCallRet(sec, ln,
                            new Class<?>[]{String.class,String.class,String.class}, user, pass, domain);
                    if (r != null) return r;
                }
            } catch (Throwable ignore) {
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

        String user     = System.getProperty("HFM_USER",     asString(a.get("user")));
        String pass     = System.getProperty("HFM_PASSWORD", asString(a.get("password")));
        String cluster  = asString(a.get("cluster"));
        String provider = asString(a.get("provider"));
        String domain   = asString(a.get("domain"));
        String server   = asString(a.get("server"));

        boolean dryRun  = Boolean.parseBoolean(String.valueOf(a.getOrDefault("dryRun","false")));
        boolean noLogin = isTrue(a.get("noLogin")); // NEW: let the action do its own login

        try {
            if (!"Consolidate".equalsIgnoreCase(op)) {
                System.out.println(jsonErr("Only 'Consolidate' operation is supported"));
                System.exit(2);
            }
            if (app == null || pov == null) {
                System.out.println(jsonErr("Missing --application and/or --pov"));
                System.exit(2);
            }

            // param map with synonyms
            Map<String,Object> params = new LinkedHashMap<String,Object>();
            params.put("Application", app);
            params.put("ApplicationName", app);
            params.put("POV", pov);
            params.put("ConsolidationType", type);
            params.put("Type", type);

            if (cluster  != null) { params.put("Cluster", cluster); params.put("ClusterName", cluster); }
            if (provider != null) { params.put("Provider", provider); params.put("ProviderURL", provider); params.put("ProviderUrl", provider); }
            if (domain   != null) { params.put("Domain", domain); params.put("DomainName", domain); }
            if (server   != null) { params.put("Server", server); params.put("ServerName", server); }
            if (user     != null) { params.put("User", user); params.put("Username", user); }
            if (pass     != null) { params.put("Password", pass); }

            // Either pass a SessionInfo (our login), OR let the Action log in itself (UC4-style)
            if (!noLogin) {
                Object sessionInfo = tryLogin(user, pass, cluster, provider, domain, server);
                if (sessionInfo != null) {
                    // Object form
                    params.put("sessionInfo", sessionInfo);
                    params.put("SessionInfo", sessionInfo);
                    // String SessionID too (some builds require it)
                    try {
                        Method getSid = sessionInfo.getClass().getMethod("getSessionId");
                        Object sid = getSid.invoke(sessionInfo);
                        if (sid != null) {
                            String s = sid.toString();
                            params.put("SessionID", s);
                            params.put("SessionId", s);
                            params.put("sessionId", s);
                        }
                    } catch (Throwable ignore) {}
                }
            } // if noLogin==true we pass only User/Password/Cluster/Provider/Domain/Server

            if (dryRun) {
                System.out.println("{\"status\":\"OK\",\"message\":\"DRY RUN\",\"application\":\""+app+"\"}");
                System.exit(0);
            }

            // execute Consolidate
            Class<?> actClz = clz("oracle.epm.fm.actions.ConsolidateAction");
            Object action   = newInstance(actClz);

            Method exec = null;
            for (Method m : actClz.getMethods()) {
                if ("execute".equals(m.getName()) && m.getParameterTypes().length == 1) {
                    exec = m; break;
                }
            }
            if (exec == null) {
                System.out.println(jsonErr("Bind error: no execute(Map) on ConsolidateAction"));
                System.exit(5);
            }

            Object ok = exec.invoke(action, params);
            boolean success = !(ok instanceof Boolean) || ((Boolean) ok).booleanValue();

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
