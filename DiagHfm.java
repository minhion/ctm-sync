package com.bmc.ctm.hfmcli;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class DiagHfm {

    public static void main(String[] args) throws Exception {
        String epmHome = System.getenv("EPM_ORACLE_HOME");
        if (epmHome == null || epmHome.isEmpty()) {
            System.out.println("EPM_ORACLE_HOME env var is empty. Set it before running.");
            System.exit(1);
        }
        String actionsJar = epmHome + "\\common\\hfm\\11.1.2.0\\lib\\fm-actions.jar";
        System.out.println("Using actions jar: " + actionsJar);

        // 1) List *Action classes
        List<String> actionClasses = listActionClasses(actionsJar);
        System.out.println("\n=== Actions in fm-actions.jar (" + actionClasses.size() + ") ===");
        for (String cn : actionClasses) {
            System.out.println(cn);
        }

        // 2) Which have public execute(Map) ?
        System.out.println("\n=== Actions exposing public execute(Map) ===");
        for (String cn : actionClasses) {
            try {
                Class<?> c = Class.forName(cn);
                Method m = c.getMethod("execute", Map.class);
                System.out.println("OK  : " + cn + " :: execute(Map)");
            } catch (ClassNotFoundException e) {
                System.out.println("MISS: " + cn + " (not on classpath?)");
            } catch (NoSuchMethodException e) {
                // ignore
            }
        }

        // 3) Dump parameter names enum (what keys the action layer recognizes)
        try {
            Class<?> enumCls = Class.forName("oracle.epm.fm.actions.generated.Parameters$ParameterNames$ParameterName");
            System.out.println("\n=== ParameterNames enum constants ===");
            Object[] constants = enumCls.getEnumConstants();
            for (Object c : constants) {
                System.out.println(" - " + c.toString());
            }
        } catch (ClassNotFoundException e) {
            System.out.println("\n(ParameterNames enum not found – different build?)");
        }

        // 4) Confirm SessionInfo presence
        try {
            Class.forName("oracle.epm.fm.common.datatype.transport.SessionInfo");
            System.out.println("\nSessionInfo class: PRESENT");
        } catch (ClassNotFoundException e) {
            System.out.println("\nSessionInfo class: NOT FOUND on classpath");
        }

        // 5) Optional: try a minimal ConsolidateAction.execute(Map) to capture required-keys message
        try {
            Class<?> c = Class.forName("oracle.epm.fm.actions.ConsolidateAction");
            Object action = c.getDeclaredConstructor().newInstance();
            Map<String,Object> m = new HashMap<>();
            // fill with your current values if you want
            m.put("Application", System.getProperty("HFM_APP",""));
            m.put("ConsolidationType", System.getProperty("HFM_TYPE",""));
            m.put("POV", System.getProperty("HFM_POV",""));
            // add Cluster/User/Password if you like:
            m.put("Cluster", System.getProperty("HFM_CLUSTER",""));
            m.put("User", System.getProperty("HFM_USER",""));
            m.put("Password", System.getProperty("HFM_PASSWORD",""));

            Method exec = c.getMethod("execute", Map.class);
            Object res = exec.invoke(action, m);
            System.out.println("\nConsolidateAction.execute(Map) returned: " + res);
        } catch (Throwable t) {
            System.out.println("\nConsolidateAction probe threw: " + t.getClass().getName() + " - " + safe(t.getMessage()));
        }
    }

    private static List<String> listActionClasses(String jarPath) throws IOException {
        List<String> out = new ArrayList<>();
        try (JarFile jf = new JarFile(new File(jarPath))) {
            Enumeration<JarEntry> e = jf.entries();
            while (e.hasMoreElements()) {
                JarEntry je = e.nextElement();
                String n = je.getName();
                if (n.endsWith("Action.class") && n.startsWith("oracle/epm/fm/actions/")) {
                    String cn = n.substring(0, n.length()-6).replace('/', '.'); // strip ".class"
                    out.add(cn);
                }
            }
        }
        Collections.sort(out);
        return out;
    }

    private static String safe(String s){
        return s==null? "" : s.replace("\r"," ").replace("\n"," ");
    }
}
