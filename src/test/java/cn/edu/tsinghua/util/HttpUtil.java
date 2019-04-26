package cn.edu.tsinghua.util;

import java.io.IOException;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class HttpUtil {

  private String url;

  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

  public HttpUtil(String url) {
    this.url = url;
  }

  public Response get() throws IOException {
    Request request = new Request.Builder()
        .url(url)
        .build();

    OkHttpClient client = new OkHttpClient();
    return client.newCall(request).execute();
  }

  public Response delete() throws IOException {
    Request request = new Request.Builder()
        .url(url)
        .delete()
        .build();

    OkHttpClient client = new OkHttpClient();
    return client.newCall(request).execute();
  }

  public Response put(String json) throws IOException {
    RequestBody body = RequestBody.create(JSON, json);
    Request request = new Request.Builder()
        .url(url)
        .put(body)
        .build();

    OkHttpClient client = new OkHttpClient();
    return client.newCall(request).execute();
  }

  public Response post(String json) throws IOException {
    RequestBody body = RequestBody.create(JSON, json);
    Request request = new Request.Builder()
        .url(url)
        .post(body)
        .build();

    OkHttpClient client = new OkHttpClient();
    return client.newCall(request).execute();
  }

}
