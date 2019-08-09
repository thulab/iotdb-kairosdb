/*
package org.kairos;

import java.io.IOException;
import java.net.MalformedURLException;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.AggregatorFactory;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.TimeUnit;
import org.kairosdb.client.response.QueryResponse;

public class TestQuery {

  public static void main(String[] args) {
    try {
      HttpClient client = new HttpClient("http://192.168.8.20:6666");
      QueryBuilder builder = QueryBuilder.getInstance();
      builder.setStart(1, TimeUnit.DAYS)
//          .setEnd(1, TimeUnit.MONTHS)
          .addMetric("CY1")
          .addAggregator(AggregatorFactory.createAverageAggregator(5, TimeUnit.MINUTES));
      QueryResponse response = client.query(builder);
      System.out.println("succeed");
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

*/
