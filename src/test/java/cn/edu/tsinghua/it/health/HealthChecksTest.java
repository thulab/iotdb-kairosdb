package cn.edu.tsinghua.it.health;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.it.RestService;
import cn.edu.tsinghua.util.HttpUtil;
import okhttp3.Response;
import org.junit.BeforeClass;
import org.junit.Test;

public class HealthChecksTest {

  private static RestService restService = new RestService();

  @BeforeClass
  public static void before() {
    restService = new RestService();
    restService.start();
    while (true) {
      if (restService.isOk()) {
        break;
      }
    }
  }

  @Test
  public void testStatus() {
    String url = restService.getUrlPrefix() + "/api/v1/health/status";
    HttpUtil httpUtil = new HttpUtil(url);
    try {
      Response response = httpUtil.get();
      assert response.body() != null;
      String res = response.body().string();
      int statusCode = response.code();
      assertEquals("[\"JVM-Thread-Deadlock: OK\",\"Datastore-Query: OK\"]", res);
      assertEquals(200, statusCode);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testCheck() {
    String url = restService.getUrlPrefix() + "/api/v1/health/check";
    HttpUtil httpUtil = new HttpUtil(url);
    try {
      Response response = httpUtil.get();
      int statusCode = response.code();
      assertEquals(204, statusCode);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
