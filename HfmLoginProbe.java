package com.bmc.ctm.hfmcli;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

public final class HfmLoginProbe {

    // --- small reflect helpers ---
    private static Class<?> clz(String name) {
        try { return Class.forName(name); } catch (Throwable t) { return null; }
    }
    private static Object newInstance(Class<?> c) {
        try { return c.getDeclaredConstructor().newInstance(); }
        catch (NoSuchMethodException e) { try { return c.newInstance(); } catch (Throwable t) { return null; } }
        catch (Throwable t) { return null; }
    }
    private static Object call(Object target, String name, Class<?>[] sig, Object... args) {
        try {
            Method m = target.getClass().getMethod(name, sig);
            m.setAccessible(true);
            return m.invoke(target, args);
        } catch (Throwable t) { return null; }
    }
    private static Object callStatic(Class<?> c, String name, Class<?>[] sig, Object... args) {
        try {
            Method m = c.getMethod(name, sig);
            m.setAccessible(true);
            return m.invoke(null, args);
        } catch (Throwable t) { return null; }
    }

    private static void println(String s){ System.out.println(s); }
    private static void diag(String s){ System.out.println("[diag] " + s); }

    private static String getPropOrNull(String key) {
        String v = System.getProperty(key);
        return (v == null || v.trim().isEmpty()) ? null : v;
    }

    // try to get SessionInfo via multiple factories + methods
    private static Object tryLogin(String user, String pass, String cluster, String provider, String domain, String server) {
        String[] factoryNames = {
            "oracle.epm.fm.common.service.ServiceClientFactory",
            "oracle.epm.fm.common.service.client.ServiceClientFactory"
        };
        String[] secGetters = { "getSecurityService", "getSecurityClient", "getSecurity", "security" };
        String[] loginNames = { "login", "authenticate", "logon" };

        for (String fqn : factoryNames) {
            Class<?> facClz = clz(fqn);
            if (facClz == null) { diag("factory not found: " + fqn); continue; }

            Object factory = callStatic(facClz, "getInstance", new Class<?>[] {});
            if (factory == null) factory = newInstance(facClz);
            if (factory == null) { diag("no instance for " + fqn); continue; }

            // optional hints on factory
            if (provider != null) { call(factory,"setProvider",new Class<?>[]{String.class},provider);
                                    call(factory,"setProviderURL",new Class<?>[]{String.class},provider);
                                    call(factory,"setProviderUrl",new Class<?>[]{String.class},provider); }
            if (domain   != null) { call(factory,"setDomain",new Class<?>[]{String.class},domain);
                                    call(factory,"setDomainName",new Class<?>[]{String.class},domain); }
            if (server   != null) { call(factory,"setServer",new Class<?>[]{String.class},server);
                                    call(factory,"setServerName",new Class<?>[]{String.class},server); }
            if (cluster  != null) { call(factory,"setCluster",new Class<?>[]{String.class},cluster);
                                    call(factory,"setClusterName",new Class<?>[]{String.class},cluster); }

            Object sec = null;
            for (String g : secGetters) {
                sec = call(factory, g, new Class<?>[] {});
                if (sec != null) { diag("security getter ok: " + fqn + "." + g + "()"); break; }
            }
            if (sec == null) { diag("no security service via " + fqn); continue; }

            // mirror hints to security client
            if (provider != null) { call(sec,"setProvider",new Class<?>[]{String.class},provider);
                                    call(sec,"setProviderURL",new Class<?>[]{String.class},provider);
                                    call(sec,"setProviderUrl",new Class<?>[]{String.class},provider); }
            if (domain   != null) { call(sec,"setDomain",new Class<?>[]{String.class},domain);
                                    call(sec,"setDomainName",new Class<?>[]{String.class},domain); }
            if (server   != null) { call(sec,"setServer",new Class<?>[]{String.class},server);
                                    call(sec,"setServerName",new Class<?>[]{String.class},server); }
            if (cluster  != null) { call(sec,"setCluster",new Class<?>[]{String.class},cluster);
                                    call(sec,"setClusterName",new Class<?>[]{String.class},cluster); }

            for (String ln : loginNames) {
                // user/pass/cluster
                Object r = call(sec, ln, new Class<?>[]{String.class,String.class,String.class}, user, pass, cluster);
                if (r != null) { diag("login via " + ln + "(u,p,cluster)"); return r; }
                // user/pass
                r = call(sec, ln, new Class<?>[]{String.class,String.class}, user, pass);
                if (r != null) { diag("login via " + ln + "(u,p)"); return r; }
                // user/pass/domain
                r = call(sec, ln, new Class<?>[]{String.class,String.class,String.class}, user, pass, domain);
                if (r != null) { diag("login via " + ln + "(u,p,domain)"); return r; }
            }
            diag("no login method worked on " + fqn);
        }
        return null;
    }

    public static void main(String[] args) {
        String user     = getPropOrNull("HFM_USER");
        String pass     = getPropOrNull("H
