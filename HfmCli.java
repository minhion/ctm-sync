package com.bmc.ctm.hfmcli;

import java.lang.reflect.*;
import java.util.*;

public class HfmCli {

    public static void main(String[] args) {
        long t0 = System.currentTimeMillis();
        Map<String,String> kv = parse(args);

        if (!"consolidate".equalsIgnoreCase(kv.getOrDefault("_sub",""))) {
            fail(2, "Use: Consolidate --application APP --consolidationType TYPE --pov \"CSV\"");
        }

        final String app   = req(kv, "--application");
        final String type  = req(kv, "--consolidationType");
        final String pov   = req(kv, "--pov");
        final boolean dry  = Boolean.parseBoolean(kv.getOrDefault("--dryRun", "false"));

        // optional extras
        final String cluster  = kv.getOrDefault("--cluster",  "");
        final String server   = kv.getOrDefault("--server",   "");
        final String provider = kv.getOrDefault("--provider", "");
        final String domain   = kv.getOrDefault("--domain",   "");
        final String locale   = kv.getOrDefault("--locale",   "");

        if (dry) ok(t0, app, "Dry run OK (args validated).");

        // creds via -DHFM_USER / -DHFM_PASSWORD
        final String user = System.getProperty("HFM_USER", "");
        final String pass = System.getProperty("HFM_PASSWORD", "");
        if (user.isEmpty() || pass.isEmpty()) fail(3, "Missing -DHFM_USER / -DHFM_PASSWORD");

        // Build a base param map used by Actions
        Map<String,Object> base = new HashMap<>();
        base.put("User", user);
        base.put("Password", pass);
        if (!cluster.isEmpty())  base.put("Cluster",  cluster);
        if (!server.isEmpty())   base.put("Server",   server);
        if (!provider.isEmpty()) base.put("Provider", provider);
        if (!domain.isEmpty())   base.put("Domain",   domain);
        if (!locale.isEmpty())   base.put("Locale",   locale);

        Object session = null;
        try {
            // 1) Logon()
            session = execLogin(base);

            // 2) OpenApplication()
            Map<String,Object> openMap = new HashMap<>(base);
            openMap.put("SessionInfo", session);
            openMap.put("Application", app);
            execActionIfPresent("oracle.epm.fm.actions.OpenApplicationAction", openMap);

            // 3) SetPOV()
            Map<String,Object> povMap = new HashMap<>(openMap);
            povMap.put("POV", pov);
            execActionIfPresent("oracle.epm.fm.actions.SetPOVAction", povMap); // not all builds have this; ok to skip

            // 4) Consolidate()
            Map<String,Object> consMap = new HashMap<>(openMap);
            consMap.put("POV", pov);
            consMap.put("ConsolidationType", type);
            boolean ok = execConsolidate(consMap);
            if (!ok) fail(7, "Consolidate returned false");

            // 5) CloseApplication()  (best-effort)
            execActionIfPresent("oracle.epm.fm.actions.CloseApplicationAction", openMap);

            // 6) Logout()  (best-effort)
            Map<String,Object> logoutMap = new HashMap<>(base);
            logoutMap.put("SessionInfo", session);
            execActionIfPresent("oracle.epm.fm.actions.LogoutAction", logoutMap);

            ok(t0, app, "Consolidation invoked");
        } catch (InvocationTargetException ite) {
            Throwable root = ite.getTargetException();
            fail(6, "HFM error: " + root.getClass().getName() + ": " + root.getMessage());
        } catch (Throwable t) {
            fail(6, t.getClass().getName() + ": " + t.getMessage());
        }
    }

    /* =================== Action calls =================== */

    private static Object execLogin(Map<String,Object> base) throws Exception {
        String cls = "oracle.epm.fm.actions.LoginAction";
        Class<?> c = Class.forName(cls);
        Object action = c.getDeclaredConstructor().newInstance();
        Method exec = c.getMethod("execute", Map.class);
        // Expect SessionInfo return; if null, try to pull from map after execute
        Object session = exec.invoke(action, base);
        if (session == null && base.containsKey("SessionInfo")) session = base.get("SessionInfo");
        if (session == null) fail(5, "Login returned no SessionInfo (check User/Password/Cluster/Provider)");
        return session;
    }

    private static void execActionIfPresent(String className, Map<String,Object> params) throws Exception {
        Class<?> c;
        try { c = Class.forName(className); } catch (ClassNotFoundException e) { return; }
        Object action = c.getDeclaredConstructor().newInstance();
        Method exec = c.getMethod("execute", Map.class);
        exec.invoke(action, params); // ignore return; best-effort
    }

    private static boolean execConsolidate(Map<String,Object> params) throws Exception {
        String cls = "oracle.epm.fm.actions.ConsolidateAction";
        Class<?> c = Class.forName(cls);
        Object action = c.getDeclaredConstructor().newInstance();
        Method exec = c.getMethod("execute", Map.class);
        Object r = exec.invoke(action, params);
        return (r instanceof Boolean) ? (Boolean) r : true;
    }

    /* =================== small helpers =================== */

    private static Map<String,String> parse(String[] args){
        Map<String,String> m = new LinkedHashMap<>();
        if (args.length>0) m.put("_sub", args[0]);
        for (int i=1;i<args.length;i++){
            String a = args[i];
            if (a.startsWith("--") && i+1<args.length) m.put(a, args[++i]);
        }
        return m;
    }
    private static String req(Map<String,String> m, String k){
        String v = m.get(k);
        if (v==null || v.isEmpty()) fail(2, "Missin
