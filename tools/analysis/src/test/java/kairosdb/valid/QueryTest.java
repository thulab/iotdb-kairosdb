package kairosdb.valid;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.AggregatorFactory;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.TimeUnit;
import org.kairosdb.client.response.GetResponse;
import org.kairosdb.client.response.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTest {

  private HttpClient client;
  Logger LOGGER= LoggerFactory.getLogger(QueryTest.class);

  @Before
  public void initHttpClient() throws MalformedURLException {
    client = new HttpClient("http://223.99.13.54:8088/");
  }

  @Test
  public void queryTest() {
    try {
      QueryBuilder builder = QueryBuilder.getInstance();

      builder.setStart(1, TimeUnit.DAYS)
//          .setEnd(1, TimeUnit.MONTHS)
          .addMetric("CY1")
          .addTag("machine_id","1701")
          .addAggregator(AggregatorFactory.createAverageAggregator(5, TimeUnit.MINUTES));
      QueryResponse response = client.query(builder);
      LOGGER.info("QueryResponse body: {}",response.getBody().toString());
//      System.out.println("succeed");
      LOGGER.info("succeed");
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void queryMetric() throws IOException {
    GetResponse getResponse = client.getMetricNames();
    List<String> metrics = getResponse.getResults();
    for (String metricName : metrics) {
      System.out.println(metricName);
    }
  }
}
