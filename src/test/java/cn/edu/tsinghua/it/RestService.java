package cn.edu.tsinghua.it;

import cn.edu.tsinghua.iotdb.kairosdb.Main;
import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.util.AddressUtil;
import cn.edu.tsinghua.util.HttpUtil;
import java.io.IOException;
import java.net.ConnectException;

public class RestService extends Thread {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Override
  public void run() {
    String[] argv = {"-cf", "conf/config.properties"};
    try {
      Main.main(argv);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public String getUrlPrefix() {
    return "http://" + AddressUtil.getLocalIpAddress() + ":" + config.REST_PORT;
  }

  public boolean isOk() {
    HttpUtil httpUtil = new HttpUtil(getUrlPrefix() + "/myresource");
    try {
      if (httpUtil.get().code() == 200) {
        return true;
      }
    } catch (ConnectException ignored) {
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }
}
