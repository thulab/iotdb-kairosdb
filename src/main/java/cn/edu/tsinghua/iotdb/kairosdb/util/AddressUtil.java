package cn.edu.tsinghua.iotdb.kairosdb.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddressUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddressUtil.class);

  private AddressUtil() {
    throw new IllegalStateException("Utility class");
  }

  public static String getLocalIpAddress() {
    InetAddress ip;
    String restIp = "localhost";
    Enumeration allNetInterfaces = null;
    try {
      allNetInterfaces = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      LOGGER.error("Get Network interfaces failed because ", e);
    }
    if (allNetInterfaces != null) {
      while (allNetInterfaces.hasMoreElements()) {
        NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
        Enumeration addresses = netInterface.getInetAddresses();
        while (addresses.hasMoreElements()) {
          ip = (InetAddress) addresses.nextElement();
          if (ip instanceof Inet4Address && !ip.getHostAddress().equals("127.0.0.1")) {
            restIp = ip.getHostAddress();
          }
        }
      }
    }
    return restIp;
  }
}
