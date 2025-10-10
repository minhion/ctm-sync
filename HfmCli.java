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

        // optional
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

        // EPM instance (wires native SDK)
        final String epmInst = System.getProperty("EPM_ORACLE_INSTANCE", "");
        if (epmInst.trim().isEmpty()) fail(4, "Missing -DEPM_ORACLE_INSTANCE");

        // Base parameters passed to actions
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
            // 1) Logon/Login/Authenticate
            session = tryAuth(base, new String[]{
                "oracle.epm.fm.actions.LogonAction",
                "oracle.epm.fm.actions.LoginAction",
                "oracle.epm.fm.actions.AuthenticateAction",
                "oracle.epm.fm.actions.SignInAction"
            });

            if (session == null) fail(5, "Auth action returned no SessionInfo (check keys: User/Password[/Cluster/Provider/Server])");

            // 2) OpenApplication (if available)
            Map<String,Object> openMap = new HashMap<>(base);
            openMap.put("SessionInfo", session);
            openMap.put("Application", app);
            execActionIfPresent("oracle.epm.fm.actions.OpenApplicationAction", openMap);

            // 3) SetPOV (optional; some builds don’t ship this)
            Map<String,Object> povMap = new HashMap<>(openMap);
            povMap.put("POV", pov);
            execActionIfPresent("oracle.epm.fm.actions.SetPOVAction", povMap);

            // 4) Consolidate (confirmed on your box)
            Map<String,Object> consMap = new HashMap<>(openMap);
            consMap.put("POV", pov);
            consMap.put("ConsolidationType", type);
            boolean ok = execConsolidate(consMap);
            if (!ok) fail(7, "Consolidate returned false");

            // 5) CloseApplication (best effort)
            execActionIfPresent("oracle.epm.fm.actions.CloseApplicationAction", openMap);

            // 6) Logout (best effort)
            Map<String,Object> logoutMap = new HashMap<>(base);
            logoutMap.put("SessionInfo", session);
            execActionIfPresent("oracle.epm.fm.actions.LogoutAction", logoutMap);

            ok(t0, app, "Consolidation invoked");
        } catch (InvocationTargetException ite) {
            Throwable root = ite.getTargetException();
            fail(6, "HFM error: " + root.getClass().getName() + ": " + safe(root.getMessage()));
        } catch (Throwable t) {
            fail(6, t.getClass().getName() + ": " + safe(t.getMessage()));
        }
    }

    /* ===== auth & actions ===== */

    private static Object tryAuth(Map<String,Object> base, String[] classNames) throws Exception {
        for (String cn : classNames) {
            try {
                Class<?> c = Class.forName(cn);
                Object action = c.getDeclaredConstructor().newInstance();
                Method exec = c.getMethod("execute", Map.class);
                Object si = exec.invoke(action, base);
                if (si != null) return si;
                if (base.containsKey("SessionInfo")) return base.get("SessionInfo");
            } catch (ClassNotFoundException ignored) {
                // try next
            }
        }
        return null;
    }

    private static void execActionIfPresent(String className, Map<String,Object> params) throws Exception {
        try {
            Class<?> c = Class.forName(className);
            Object action = c.getDeclaredConstructor().newInstance();
            Method exec = c.getMethod("execute", Map.class);
            exec.invoke(action, params);
        } catch (ClassNotFoundException ignored) {
            // action not present in this build → skip
        }
    }

    private static boolean execConsolidate(Map<String,Object> params) throws Exception {
        String cls = "oracle.epm.fm.actions.ConsolidateAction";
        Class<?> c = Class.forName(cls);
        Object action = c.getDeclaredConstructor().newInstance();
        Method exec = c.getMethod("execute", Map.class);
        Object r = exec.invoke(action, params);
        return (r instanceof Boolean) ? (Boolean) r : true;
    }

    /* ===== misc ==== */

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
        if (v==null || v.isEmpty()) fail(2, "Missing arg: "+k);
        return v;
    }
    private static void ok(long t0, String app, String msg){
        long ms = System.currentTimeMillis()-t0;
        System.out.println("{\"status\":\"OK\",\"application\":\""+esc(app)+"\",\"elapsed_ms\":"+ms+",\"message\":\""+esc(msg)+"\"}");
        System.exit(0);
    }
    private static void fail(int code, String msg){
        System.out.println("{\"status\":\"Error\",\"message\":\""+esc(msg)+"\"}");
        System.exit(code);
    }
    private static String esc(String s){ return s==null?"":s.replace("\"","\\\""); }
    private static String safe(String s){ return s==null?"":s.replace("\"","\\\"").replace("\n"," ").replace("\r"," "); }
}
