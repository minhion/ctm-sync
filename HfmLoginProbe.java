// HfmLoginProbe.java
import oracle.epm.fm.common.service.ServiceClientFactory;
import oracle.epm.fm.common.datatype.transport.SessionInfo;

public class HfmLoginProbe {
  public static void main(String[] a) throws Exception {
    String user = System.getProperty("HFM_USER");
    String pass = System.getProperty("HFM_PASSWORD");
    String cluster = (a.length > 0 ? a[0] : null);
    if (user==null || pass==null || cluster==null) {
      System.out.println("Need HFM_USER,HFM_PASSWORD and arg[0]=CLUSTER");
      System.exit(2);
    }
    // Depending on build, this may be getSecurityService() or getSecurityClient()
    Object sec = null;
    try { sec = ServiceClientFactory.getInstance().getSecurityService(); } catch (Throwable ignore) {}
    if (sec == null) {
      sec = ServiceClientFactory.getInstance().getSecurityClient();
    }
    SessionInfo si = null;
    try { si = (SessionInfo)sec.getClass().getMethod("login", String.class,String.class,String.class).invoke(sec, user, pass, cluster); }
    catch (NoSuchMethodException e) {
      // try two-arg signature
      si = (SessionInfo)sec.getClass().getMethod("login", String.class,String.class).invoke(sec, user, pass);
    }
    System.out.println("SessionInfo: " + si);
    if (si != null) {
      System.out.println("SessionId=" + si.getSessionId() + " server=" + si.getServerName() + ":" + si.getPortNum());
      System.exit(0);
    } else {
      System.exit(5);
    }
  }
}
