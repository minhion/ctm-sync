package com.bmc.ctm.hfmcli;

import java.lang.reflect.*;
import java.util.*;

/**
 * HfmCli Consolidate --application <APP> --consolidationType <TYPE> --pov "<CSV>" [--dryRun true|false]
 * Pass credentials via -DHFM_USER=... -DHFM_PASSWORD=...
 * Requires runtime CP to include:
 *   epm_hfm_web.jar, epm_j2se.jar, epm_thrift.jar
 *   fm-web-objectmodel.jar, fmcommon.jar, fm-actions.jar, fm-adm-driver.jar
 * And -DEPM_ORACLE_INSTANCE=... plus PATH including EPM native DLL folders.
 */
public class HfmCli {

    public static void main(String[] args) {
        long t0 = System.currentTimeMillis();
        Map<String,String> kv = parse(args);

        if (!"consolidate".equalsIgnoreCase(kv.get("_sub"))) {
            fail(2, "Use: Consolidate");
        }
        final String app  = req(kv, "--application");
        final String type = req(kv, "--consolidationType");
        final String pov  = req(kv, "--pov");
        final boolean dry = Boolean.parseBoolean(kv.getOrDefault("--dryRun", "false"));

        final String epmInst = System.getProperty("EPM_ORACLE_INSTANCE", "");
        if (epmInst.trim().isEmpty()) fail(3, "Missing -DEPM_ORACLE_INSTANCE");

        if (dry) ok(t0, app, "Dry run OK (args validated).");

        // Credentials (non-SSO)
        final String user = System.getProperty("HFM_USER", "");
        final String pass = System.getProperty("HFM_PASSWORD", "");
        if (user.isEmpty() || pass.isEmpty()) {
            fail(4, "Missing -DHFM_USER / -DHFM_PASSWORD");
        }

        // Try strategy A: Action framework (oracle.epm.fm.actions.ConsolidateAction)
        StringBuilder debug = new StringBuilder();
        try {
            Object result = tryConsolidateViaActions(app, type, pov, user, pass, debug);
            ok(t0, app, "Consolidation invoked via fm-actions (" + result + ")");
            return;
        } catch (Throwable a) {
            debug.append("[A fail] ").append(a.getClass().getSimpleName()).append(": ").append(a.getMessage()).append("\n");
        }

        // Try strategy B: ServiceClientFactory + Application/ProcessControl services
        try {
            Object result = tryConsolidateViaServices(app, type, pov, user, pass, debug);
            ok(t0, app, "Consolidation invoked via services (" + result + ")");
            return;
        } catch (Throwable b) {
            debug.append("[B fail] ").append(b.getClass().getSimpleName()).append(": ").append(b.getMessage()).append("\n");
        }

        // If we get here, we couldn’t bind to live SDK methods using the two common paths
        fail(5, "Bind error. Adjust class/method names. Details:\n" + debug.toString());
    }

    /* ---------- Strategy A: fm-actions ConsolidateAction ---------- */
    private static Object tryConsolidateViaActions(String app, String type, String povCsv, String user, String pass, StringBuilder dbg) throws Exception {
        // Known class present on your system:
        //   oracle.epm.fm.actions.ConsolidateAction
        Class<?> cAction = Class.forName("oracle.epm.fm.actions.ConsolidateAction");
        Object action = newInstance(cAction);

        // Heuristic setters (we’ll try common names; missing ones are skipped)
        setIfExists(action, "setApplication", String.class, app);
        setIfExists(action, "setAppName",    String.class, app);
        setIfExists(action, "setConsolidationType", String.class, type);
        setIfExists(action, "setType", String.class, type);
        setIfExists(action, "setPOV", String.class, povCsv);
        setIfExists(action, "setPov", String.class, povCsv);
        setIfExists(action, "setUser", String.class, user);
        setIfExists(action, "setUsername", String.class, user);
        setIfExists(action, "setPassword", String.class, pass);

        // Many Oracle “Action” objects are executed via a generic runner/dispatcher.
        // Try common patterns:
        // 1) action.execute() / run() / perform()
        Object res = tryInvokeNoArg(action, "execute");
        if (res == null) res = tryInvokeNoArg(action, "run");
        if (res == null) res = tryInvokeNoArg(action, "perform");

        if (res != null) return res;

        // 2) Some actions require a context/session passed in (SessionInfo or similar).
        // Build a lightweight SessionInfo by reflection and pass if a suitable method exists.
        Object session = buildSessionInfo(app, user, pass);
        res = tryInvoke(action, "execute", new Class<?>[]{session.getClass()}, session);
        if (res != null) return res;
        res = tryInvoke(action, "run", new Class<?>[]{session.getClass()}, session);
        if (res != null) return res;
        res = tryInvoke(action, "perform", new Class<?>[]{session.getClass()}, session);
        if (res != null) return res;

        throw new NoSuchMethodException("ConsolidateAction: no usable execute/run/perform method found.");
    }

    /* ---------- Strategy B: ServiceClientFactory + services ---------- */
    private static Object tryConsolidateViaServices(String app, String type, String povCsv, String user, String pass, StringBuilder dbg) throws Exception {
        // Factory & service enums
        Class<?> cFactory = Class.forName("oracle.epm.fm.common.service.ServiceClientFactory");
        Class<?> cSvcType = Class.forName("oracle.epm.fm.common.service.ServiceType");
        Object factory = cFactory.getMethod("getInstance").invoke(null);

        // Build SessionInfo (transport object)
        Object session = buildSessionInfo(app, user, pass);

        // APPLICATION service (open app / validate)
        Object svcApp = getServiceClient(factory, cSvcType, "APPLICATION");
        // Try common “openApplication” signatures
        invokeIfExists(svcApp, "openApplication", new Class<?>[]{String.class}, app);
        invokeIfExists(svcApp, "openApplication", new Class<?>[]{session.getClass()}, session);

        // PROCESSCONTROL or CONSOLIDATION service (naming differs by build)
        Object svcProc = getServiceClient(factory, cSvcType, "PROCESSCONTROL");
        if (svcProc == null) {
            svcProc = getServiceClient(factory, cSvcType, "CONSOLIDATION");
        }
        if (svcProc == null) {
            // Some builds bundle into Application service
            svcProc = svcApp;
        }

        // Build POV object if SDK exposes a Pov type; else pass CSV
        Object povObj = buildPovIfPossible(povCsv);

        // Try common consolidate signatures
        Object res = tryInvoke(svcProc, "consolidate", new Class<?>[]{String.class, povObj.getClass(), String.class}, app, povObj, type);
        if (res != null) return res;

        res = tryInvoke(svcProc, "consolidate", new Class<?>[]{session.getClass(), povObj.getClass(), String.class}, session, povObj, type);
        if (res != null) return res;

        res = tryInvoke(svcProc, "consolidate", new Class<?>[]{String.class, String.class, String.class}, app, povCsv, type);
        if (res != null) return res;

        // Fallback: any 2 or 3 arg “consolidate” we can find
        Method m = findMethodByName(svcProc.getClass(), "consolidate");
        if (m != null) {
            Object[] args = coerceArgs(m.getParameterTypes(), app, povObj, povCsv, type, session);
            return m.invoke(svcProc, args);
        }

        throw new NoSuchMethodException("No usable consolidate(...) on service client.");
    }

    /* ---------- Helpers ---------- */
    private static Map<String,String> parse(String[] args){
        Map<String,String> m = new LinkedHashMap<>();
        if (args.length > 0) m.put("_sub", args[0]);
        for (int i=1;i<args.length;i++){
            if (args[i].startsWith("--") && i+1<args.length) m.put(args[i], args[++i]);
        }
        return m;
    }
    private static String req(Map<String,String> m, String k){
        String v = m.get(k);
        if (v==null || v.isEmpty()) fail(2, "Missing required arg: "+k);
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

    private static Object newInstance(Class<?> c) throws Exception {
        Constructor<?> k = c.getDeclaredConstructor();
        k.setAccessible(true);
        return k.newInstance();
    }
    private static void setIfExists(Object target, String method, Class<?> t, Object v){
        try {
            Method m = target.getClass().getMethod(method, t);
            m.invoke(target, v);
        } catch (Throwable ignored){}
    }
    private static Object tryInvokeNoArg(Object target, String name){
        try {
            Method m = target.getClass().getMethod(name);
            return m.invoke(target);
        } catch (Throwable t){ return null; }
    }
    private static Object tryInvoke(Object target, String name, Class<?>[] sig, Object... args){
        try {
            Method m = target.getClass().getMethod(name, sig);
            return m.invoke(target, args);
        } catch (Throwable t){ return null; }
    }
    private static void invokeIfExists(Object target, String name, Class<?>[] sig, Object... args){
        try { target.getClass().getMethod(name, sig).invoke(target, args); } catch (Throwable ignored){}
    }
    private static Method findMethodByName(Class<?> c, String name){
        for (Method m: c.getMethods()){
            if (m.getName().equals(name)) return m;
        }
        return null;
    }
    private static Object getServiceClient(Object factory, Class<?> cSvcType, String enumConst) {
        try {
            Object svcEnum = Enum.valueOf((Class<Enum>)cSvcType, enumConst);
            Method m = factory.getClass().getMethod("getServiceClient", cSvcType);
            return m.invoke(factory, svcEnum);
        } catch (Throwable t){
            return null;
        }
    }

    private static Object buildSessionInfo(String app, String user, String pass) throws Exception {
        // oracle.epm.fm.common.datatype.transport.SessionInfo (present in your fmcommon.jar)
        Class<?> cSI = Class.forName("oracle.epm.fm.common.datatype.transport.SessionInfo");
        Object si = newInstance(cSI);
        // try common setters
        setIfExists(si, "setAppName", String.class, app);
        setIfExists(si, "setApplicationName", String.class, app);
        setIfExists(si, "setUser", String.class, user);
        setIfExists(si, "setUsername", String.class, user);
        setIfExists(si, "setPassword", String.class, pass);
        return si;
    }

    private static Object buildPovIfPossible(String povCsv) throws Exception {
        // Try a known transport type first (if available),
        // else return the CSV and rely on a consolidate(String,String,String) signature.
        // Candidates seen in your jars: oracle.epm.fm.common.datatype.transport.POVDimension, and various domain POVs.
        try {
            // If there is a static parser like Pov.fromCsv(String)
            Class<?> cPov = Class.forName("oracle.epm.fm.domainobject.pov.Pov");
            try {
                Method from = cPov.getMethod("fromCsv", String.class);
                return from.invoke(null, povCsv);
            } catch (NoSuchMethodException ignored) {
                // fall through
            }
        } catch (ClassNotFoundException ignore){}

        // Another common transport form (placeholder): use raw CSV
        return povCsv;
    }

    private static Object[] coerceArgs(Class<?>[] paramTypes, String app, Object povObj, String povCsv, String type, Object session){
        List<Object> out = new ArrayList<>();
        for (Class<?> pt: paramTypes){
            if (pt == String.class){
                // choose best string
                if (!out.contains(app))       { out.add(app); continue; }
                if (!out.contains(type))      { out.add(type); continue; }
                if (!out.contains(povCsv))    { out.add(povCsv); continue; }
                out.add("");
            } else if (session!=null && pt.isInstance(session)){
                out.add(session);
            } else if (povObj!=null && pt.isInstance(povObj)){
                out.add(povObj);
            } else {
                out.add(null);
            }
        }
        return out.toArray(new Object[0]);
    }
}
