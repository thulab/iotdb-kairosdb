package kairosdb.valid.ana;

import java.io.IOException;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import kairosdb.valid.ana.conf.CommandCli;
import kairosdb.valid.ana.conf.Config;
import kairosdb.valid.ana.conf.ConfigDescriptor;
import kairosdb.valid.ana.conf.Constants;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.AggregatorFactory;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.TimeUnit;
import org.kairosdb.client.response.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataAnalysis {

  private static Config config;
  private static long startTime;
  private static long endTime;
  static Logger LOGGER = LoggerFactory.getLogger(DataAnalysis.class);

  public static void main(String[] args) throws ParseException, IOException {
    CommandCli cli = new CommandCli();
    if (!cli.init(args)) {
      System.exit(1);
    }
    config = ConfigDescriptor.getInstance().getConfig();
    startTime = dateToTimestamp(config.START_TIME_);
    endTime = dateToTimestamp(config.END_TIME_);
    HttpClient kairosCli = getKairosCli();
    HttpClient ikrCli = getIKRCli();
    long intervalNum = getIntervalNum(startTime, endTime);
    for (int i = 0; i < intervalNum; i++) {
      long start;
      long end;
      if (i == intervalNum - 1) {
        start = startTime + i * Constants.TIME_INTERVAL;
        end = endTime;
      } else {
        start = startTime + i * Constants.TIME_INTERVAL;
        end = start + Constants.TIME_INTERVAL;
      }
      Date startDate = stampToDate(start);
      Date endDate = stampToDate(end);
      for (String metric : config.METRICS) {
        dataValid(kairosCli, ikrCli, metric, startDate, endDate);
      }
    }

    LOGGER.info("数据验证结束");
  }

  public static HttpClient getKairosCli() {
    try {
      return new HttpClient(config.KAIROS_URL);
    } catch (MalformedURLException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static HttpClient getIKRCli() {
    try {
      return new HttpClient(config.IKR_URL);
    } catch (MalformedURLException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static long dateToTimestamp(String time) throws ParseException {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date date = format.parse(time);
    //日期转时间戳（毫秒）
    return date.getTime();
  }

  public static long getIntervalNum(long start, long end) {
    if ((end - start) % Constants.TIME_INTERVAL == 0) {
      return (end - start) / Constants.TIME_INTERVAL;
    } else {
      return (end - start) / Constants.TIME_INTERVAL + 1;
    }
  }

  public static Date stampToDate(long timestamp) {
    return new Date(timestamp);
  }

  public static void dataValid(HttpClient kairosCli, HttpClient ikrCli, String metric, Date start,
      Date end) throws IOException {
    QueryBuilder builder = QueryBuilder.getInstance();

    builder.setStart(start).setEnd(end).addMetric(metric).addTag("machine_id", config.TAG);
    QueryResponse kairosResponse = kairosCli.query(builder);
    QueryResponse ikrResponse = ikrCli.query(builder);
//    if (!kairosResponse.getBody().toString().equals(ikrResponse.getBody().toString())) {
//      LOGGER.error("metric {} 在时间 {} 至 {} 内数据不一致", metric, start, end);
//    }
    if (kairosResponse.getQueries().get(0).getSampleSize() != ikrResponse.getQueries().get(0).getSampleSize()){
      LOGGER.error("metric {} 在时间 {} 至 {} 内数据不一致", metric, start, end);
    }
  }

}
