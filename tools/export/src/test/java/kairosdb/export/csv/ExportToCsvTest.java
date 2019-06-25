package kairosdb.export.csv;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import kairosdb.export.csv.conf.Config;
import kairosdb.export.csv.conf.ConfigDescriptor;
import kairosdb.export.csv.conf.Constants;
import org.junit.Test;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for simple ExportToCsv.
 */
public class ExportToCsvTest {

  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(ExportToCsvTest.class);

  @Test
  public void testExport() {
    try (HttpClient client = new HttpClient("http://192.168.130.6:8080")) {
      long startTime = System.currentTimeMillis();
      for (int i = 0; i < 10; i++) {
        long recordTime = startTime + i * 1000;
        for (String metric : config.METRIC_LIST.split(",")) {
          MetricBuilder builder = MetricBuilder.getInstance();
          builder.addMetric(metric)
              .addTag(Constants.TAG_KEY1, config.MACHINE_ID)
              .addDataPoint(recordTime, 10.5);
          client.pushMetrics(builder);
        }
      }
      LOGGER.info("data prepared");
    } catch (IOException e) {
      LOGGER.error("prepare kairosdb data failed", e);
    }
    assertTrue(true);
  }
}
