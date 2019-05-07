package cn.edu.tsinghua.it.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import cn.edu.tsinghua.it.RestService;
import cn.edu.tsinghua.util.HttpUtil;
import java.io.IOException;
import okhttp3.Response;
import org.junit.BeforeClass;
import org.junit.Test;

public class MetadataTest {

  private static String url;

  @BeforeClass
  public static void before() {
    RestService restService = new RestService();
    url = restService.getUrlPrefix() + "/api/v1/metadata";
    restService.start();
    while (true) {
      if (restService.isOk()) {
        break;
      }
    }
  }

  @Test
  public void testMetadataService() {
    addValue();
    getValue();
    listKeys();
    listServiceKeys();
    deleteKey();
  }

  private void addValue() {
    try {
      Response response = new HttpUtil(getUrl("t_service", "t_service_key", "t_key")).post("value");
      assertEquals(204, response.code());
      response = new HttpUtil(getUrl("t_service", "t_service_key", "t_key")).post("value2");
      assertEquals(204, response.code());
      response = new HttpUtil(getUrl("t_service", "t_service_key", "t_key2")).post("value3");
      assertEquals(204, response.code());
      response = new HttpUtil(getUrl("t_service", "t_service_key", "t_key2")).post("value4");
      assertEquals(204, response.code());
      response = new HttpUtil(getUrl("t_service", "t_service_key2", "t_key3")).post("value5");
      assertEquals(204, response.code());
      response = new HttpUtil(getUrl("t_service", "t_service_key2", "t_key4")).post("value6");
      assertEquals(204, response.code());

      getValue();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void getValue() {
    try {
      Response response = new HttpUtil(getUrl("t_service", "t_service_key", "t_key")).get();
      assertEquals(200, response.code());
      assertNotNull(response.body());
      assertEquals("value2", response.body().string());
      response = new HttpUtil(getUrl("t_service", "t_service_key", "t_key2")).get();
      assertEquals(200, response.code());
      assertNotNull(response.body());
      assertEquals("value4", response.body().string());
      response = new HttpUtil(getUrl("t_service", "t_service_key2", "t_key3")).get();
      assertEquals(200, response.code());
      assertNotNull(response.body());
      assertEquals("value5", response.body().string());
      response = new HttpUtil(getUrl("t_service", "t_service_key2", "t_key4")).get();
      assertEquals(200, response.code());
      assertNotNull(response.body());
      assertEquals("value6", response.body().string());

      response = new HttpUtil(getUrl("t_service1", "t_service_key2", "t_key5")).get();
      assertEquals(200, response.code());
      assertNotNull(response.body());
      assertEquals("", response.body().string());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void listServiceKeys() {
    try {
      Response response = new HttpUtil(getUrl("t_service", null, null)).get();
      assertEquals(200, response.code());
      assertNotNull(response.body());
      assertEquals("{\"results\":[\"t_service_key\",\"t_service_key2\"]}",
          response.body().string());
      response = new HttpUtil(getUrl("t_service2", null, null)).get();
      assertEquals(200, response.code());
      assertNotNull(response.body());
      assertEquals("{\"results\":[]}", response.body().string());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void listKeys() {
    try {
      Response response = new HttpUtil(getUrl("t_service", "t_service_key", null)).get();
      assertEquals(200, response.code());
      assertNotNull(response.body());
      response = new HttpUtil(getUrl("t_service", "t_service_key2", null)).get();
      assertEquals(200, response.code());
      assertNotNull(response.body());
      response = new HttpUtil(getUrl("t_service", "t_service_key3", null)).get();
      assertEquals(200, response.code());
      assertNotNull(response.body());
      assertEquals("{\"results\":[]}", response.body().string());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void deleteKey() {
    try {
      Response response = new HttpUtil(getUrl("t_service", "t_service_key", "t_key")).get();
      assertEquals(200, response.code());
      assertNotNull(response.body());
      assertEquals("value2", response.body().string());
      response = new HttpUtil(getUrl("t_service", "t_service_key", "t_key")).delete();
      assertEquals(204, response.code());
      response = new HttpUtil(getUrl("t_service", "t_service_key", "t_key")).get();
      assertEquals(200, response.code());
      assertNotNull(response.body());
      assertEquals("", response.body().string());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String getUrl(String service, String serviceKey, String key) {
    if (serviceKey == null) {
      return String.format("%s/%s", url, service);
    }
    if (key == null) {
      return String.format("%s/%s/%s", url, service, serviceKey);
    }
    return String.format("%s/%s/%s/%s", url, service, serviceKey, key);
  }

}
