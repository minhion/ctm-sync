package com.bmc.ctm.hfmcli;

import java.util.*;
import oracle.epm.fm.actions.ConsolidateAction;
import oracle.epm.fm.common.exception.HFMException;

public class HfmCli {
    public static void main(String[] args) {
        long t0 = System.currentTimeMillis();
        Map<String,String> kv = parse(args);

        if (!"Consolidate".equalsIgnoreCase(kv.get("_sub"))) {
            fail(2, "Use: Consolidate");
        }

        final String app  = req(kv, "--application");
        final String type = req(kv, "--consolidationType");
        final String pov  = req(kv, "--pov");
        final boolean dry = Boolean.parseBoolean(kv.getOrDefault("--dryRun", "false"));

        if (dry) ok(t0, app, "Dry run OK (args validated).");

        // creds from sysprops
        final String user = System.getProperty("HFM_USER", "");
        final String pass = System.getProperty("HFM_PASSWORD", "");
        if (user.isEmpty() || pass.isEmpty()) fail(3, "Missing -DHFM_USER/-DHFM_PASSWORD");

        try {
            ConsolidateAction action = new ConsolidateAction();
            Map<String,String> params = new HashMap<>();
            params.put("Application", app);
            params.put("ConsolidationType", type);
            params.put("POV", pov);
            params.put("User", user);
            params.put("Password", pass);

            boolean result = action.execute(params);
            ok(t0, app, "Consolidation executed, result=" + result);
        } catch (HFMException e) {
            fail(5, "HFMException: " + e.getMessage());
        } catch (Throwable t) {
            fail(6, "Unexpected: " + t.getClass().getName() + ": " + t.getMessage());
        }
    }

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
        if (v==null || v.isEmpty()) fail(2, "Missing arg: "+k);
        return v;
    }
    private static void ok(long t0, String app, String msg){
        long ms = System.currentTimeMillis()-t0;
        System.out.println("{\"status\":\"OK\",\"application\":\""+app+"\",\"elapsed_ms\":"+ms+",\"message\":\""+msg+"\"}");
        System.exit(0);
    }
    private static void fail(int code, String msg){
        System.out.println("{\"status\":\"Error\",\"message\":\""+msg+"\"}");
        System.exit(code);
    }
}
