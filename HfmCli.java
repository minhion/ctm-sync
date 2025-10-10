package com.bmc.ctm.hfmcli;

import java.lang.reflect.*;
import java.util.*;

public class HfmCli {

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

  private static Map<String,Object> argMap(String[] argv) {
    Map<String,Object> out = new LinkedHashMap<>();
    String k = null;
    for (String a : argv) {
      if (a.startsWith("--")) {
        k = a.substring(2);
        out.put(k, Boolean.TRUE); // default
      } else if (k != null) {
        out.put(k, a);
        k = null;
      }
    }
    return out;
  }

  public static void main(String[] args) {
    long t0 = System.currentTimeMillis();
    Map<String,Object> a = argMap(args);

    String op   = args.length > 0 ? args[0] : "Consolidate";
    String app  = (String) a.get("application");
    String type = (String) a.getOrDefault("consolidationType","AllWithData");
    String pov  = (String) a.get("pov");

    // creds (prefer -D props set by CTM wrapper, otherwise --user/--password/--cluster)
    String user    = System.getProperty("HFM_USER",    (String) a.get("user"));
    String pass    = System.getProperty("HFM_PASSWORD",(String) a.get("password"));
    String cluster = (String) a.get("cluster");

    boolean dryRun = Boolean.parseBoolean(String.valueOf(a.getOrDefault("dryRun","false")));

    try {
      if (!"Consolidate".equalsIgnoreCase(op)) {
        System.out.println(jsonErr("Only Consolidate supported right now")); System.exit(2);
      }
      if (app==null || pov==null) {
        System.out.println(jsonErr("Missing --application and/or --pov")); System.exit(2);
      }
      // Build parameters map expected by ConsolidateAction
      Map<String,Object> params = new LinkedHashMap<>();
      params.put("Application", app);
      params.put("POV", pov);
      params.put("Type", type);

      // ---- Login to obtain SessionInfo (critical) ----
      Object sessionInfo = null;
      if (user!=null && pass!=null) {
        sessionInfo = tryLogin(user, pass, cluster);
        if (sessionInfo == null) {
          System.out.println(jsonErr("Auth action returned no SessionInfo (check keys: User/Password[/Cluster])"));
          System.exit(5);
        }
        // common keys used by vendor tooling
        params.put("sessionInfo", sessionInfo);
        params.put("SessionInfo", sessionInfo);
      }

      if (dryRun) {
        System.out.println("{\"status\":\"OK\",\"message\":\"DRY RUN\",\"application\":\""+app+"\"}");
        System.exit(0);
      }

      // ---- Execute ConsolidateAction ----
      Class<?> actClz = clz("oracle.epm.fm.actions.ConsolidateAction");
      Object action = newInstance(actClz);

      // method: boolean execute(Map<String,String>) — some builds accept (Map) with non-String values
      Method exec = null;
      for (Method m : actClz.getMethods()) {
        if (m.getName().equals("execute") && m.getParameterCount()==1) {
          exec = m; break;
        }
      }
      if (exec == null) {
        System.out.println(jsonErr("Bind error: no execute(Map) on ConsolidateAction"));
        System.exit(5);
      }
      Object ok = exec.invoke(action, params);
      boolean success = (ok instanceof Boolean) ? (Boolean) ok : true;

      long ms = System.currentTimeMillis() - t0;
      if (success) {
        System.out.println("{\"status\":\"OK\",\"application\":\""+app+"\",\"elapsed_ms\":"+ms+"}");
        System.exit(0);
      } else {
        System.out.println(jsonErr("Consolidate returned false"));
        System.exit(6);
      }
    } catch (Throwable t) {
      t.printStackTrace();
      System.out.println(jsonErr(t.getClass().getSimpleName()+": "+t.getMessage()));
      System.exit(5);
    }
  }

  private static Object tryLogin(String user, String pass, String cluster) {
    try {
      // Typical path: ServiceClientFactory -> SecurityService -> login/authenticate -> SessionInfo
      // Try several common signatures via reflection
      Class<?> factoryClz = clz("oracle.epm.fm.common.service.ServiceClientFactory");
      Object  factory     = newInstance(factoryClz);

      // try getSecurityService() / getSecurityClient()
      Object sec = null;
      for (String m : new String[]{"getSecurityService","getSecurityClient"}) {
        try { sec = call(factory, m, new Class<?>[]{}); if (sec!=null) break; } catch (Throwable ignore) {}
      }
      if (sec == null) return null;

      // candidate auth method names
      String[] names = {"login","authenticate","logon"};
      // candidate signatures: (String,String,String) and (String,String)
      for (String m : names) {
        try {
          return call(sec, m, new Class<?>[]{String.class,String.class,String.class}, user, pass, cluster);
        } catch (NoSuchMethodException e) { /* try next */ }
        catch (Throwable ok) { return ok; }
        try {
          return call(sec, m, new Class<?>[]{String.class,String.class}, user, pass);
        } catch (NoSuchMethodException e) { /* try next */ }
        catch (Throwable ok) { return ok; }
      }
    } catch (Throwable ignore) {
      // fall through
    }
    return null;
  }

  private static String jsonErr(String m) {
    return "{\"status\":\"Error\",\"message\":\""+m.replace("\"","'")+"\"}";
  }
}
