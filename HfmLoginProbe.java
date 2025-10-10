package com.bmc.ctm.hfmcli;

import oracle.epm.fm.common.service.ServiceClientFactory;
import oracle.epm.fm.common.datatype.transport.SessionInfo;

public final class HfmLoginProbe {
  public static void main(String[] args) {
    try {
      String user    = System.getProperty("HFM_USER");
      String pass    = System.getProperty("HFM_PASSWORD");
      String cluster = (args.length > 0 ? args[0] : null);

      if (user == null || pass == null || cluster == null) {
        System.out.println("{\"status\":\"Error\",\"message\":\"Need HFM_USER,HFM_PASSWORD system properties and arg0=CLUSTER\"}");
        System.exit(2);
      }

      Object sec = null;
      try {
        sec = ServiceClientFactory.getInstance().getSecurityService();
        System.out.println("Using SecurityService");
      } catch (Throwable t) {
        System.out.println("getSecurityService() failed: " + t.getClass().getSimpleName());
      }
      if (sec == null) {
        try {
          sec = ServiceClientFactory.getInstance().getSecurityClient();
          System.out.println("Using SecurityClient");
        } catch (Throwable t) {
          System.out.println("getSecurityClient() failed: " + t.getClass().getSimpleName());
        }
      }
      if (sec == null) {
        System.out.println("{\"status\":\"Error\",\"message\":\"No Security service available from ServiceClientFactory\"}");
        System.exit(5);
      }

      SessionInfo si = null;
      try {
        si = (SessionInfo) sec.getClass()
              .getMethod("login", String.class, String.class, String.class)
              .invoke(sec, user, pass, cluster);
        System.out.println("login(user,pass,cluster) worked");
      } catch (NoSuchMethodException e) {
        System.out.println("3-arg login not found, trying 2-arg...");
        try {
          si = (SessionInfo) sec.getClass()
                .getMethod("login", String.class, String.class)
                .invoke(sec, user, pass);
        } catch (Throwable t2) {
          System.out.println("2-arg login failed: " + t2.getClass().getSimpleName() + ": " + safe(t2.getMessage()));
        }
      } catch (Throwable t) {
        System.out.println("3-arg login failed: " + t.getClass().getSimpleName() + ": " + safe(t.getMessage()));
      }

      if (si == null) {
        System.out.println("{\"status\":\"Error\",\"message\":\"Login returned null SessionInfo\"}");
        System.exit(5);
      }

      System.out.println("{\"status\":\"OK\",\"sessionId\":\"" + safe(si.getSessionId())
        + "\",\"server\":\"" + safe(si.getServerName())
        + "\",\"port\":" + si.getPortNum()
        + ",\"xdsPort\":" + si.getXdsManagementPortNum() + "}");
      System.exit(0);
    } catch (Throwable t) {
      t.printStackTrace(System.err);
      System.out.println("{\"status\":\"Error\",\"message\":\"" + t.getClass().getSimpleName() + ": " + safe(t.getMessage()) + "\"}");
      System.exit(5);
    }
  }

  private static String safe(String s) { return s == null ? "" : s.replace("\"","'"); }
}
