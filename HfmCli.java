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

        final String cluster  = kv.getOrDefault("--cluster",  "");
        final String server   = kv.getOrDefault("--server",   "");
        final String provider = kv.getOrDefault("--provider", "");
        final String domain   = kv.getOrDefault("--domain",   "");
        final String locale   = kv.getOrDefault("--locale",   "");

        if (dry) ok(t0, app, "Dry run OK (args validated).");

        final String user = System.getProperty("HFM_USER", "");
        final String pass = System.getProperty("HFM_PASSWORD", "");
        if (user.isEmpty() || pass.isEmpty()) fail(3, "Missing -DHFM_USER / -DHFM_PASSWORD");

        final String epmInst = System.getProperty("EPM_ORACLE_INSTANCE", "");
        if (epmInst.trim().isEmpty()) fail(4, "Missing -DEPM_ORACLE_INSTANCE");

        // Base map used across actions; include many key variants to satisfy different builds
        Map<String,Object> base = new HashMap<>();
        base.put("User", user);
        base.put("Username", user);
        base.put("Password", pass);
        if (!cluster.isEmpty())  base.put("Cluster",  cluster);
        if (!server.isEmpty())   base.put("Server",   server);
        if (!provider.isEmpty()) base.put("Provider", provider);
        if (!domain.isEmpty())   base.put("Domain",   domain);
        if (!locale.isEmpty())   base.put("Locale",   locale);

        Object session = null;
        try {
            // 1) AUTH
            session = tryAuth(base, new String[]{
                "oracle.epm.fm.actions.LogonAction",
                "oracle.epm.fm.actions.LoginAction",
                "oracle.epm.fm.actions.AuthenticateAction",
                "oracle.epm.fm.actions.SignInAction"
            });

            if (session == null) {
                fail(5, "Auth action returned no SessionInfo (try passing --cluster/--provider/--server or verify account)");
            }

            // 2) OPEN APP (best effort)
            Map<String,Object> openMap = new HashMap<>(base);
            openMap.put("SessionInfo", session);
            // support app name under multiple keys
            openMap.put("Application", app);
            openMap.put("AppName", app);
            openMap.put("ApplicationName", app);
            execActionIfPresent("oracle.epm.fm.actions.OpenApplicationAction", openMap);

            // 3) SET POV (best effort)
            Map<String,Object> povMap = new HashMap<>(openMap);
            povMap.put("POV", pov);
            execActionIfPresent("oracle.epm.fm.actions.SetPOVAction", povMap);

            // 4) CONSOLIDATE (confirmed in your build)
            Map<String,Object> consMap = new HashMap<>(openMap);
            consMap.put("POV", pov);
            consMap.put("ConsolidationType", type);
            boolean ok = execConsolidate(consMap);
            if (!ok) fail(7, "Consolidate returned false");

            // 5) CLOSE + LOGOUT (best effort)
