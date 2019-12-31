package kairosdb.export.csv.utils;

import kairosdb.export.csv.conf.Constants;
import org.joda.time.DateTime;

public class TimeUtils {

  public static long generateTimestamp(long startTime, long interval, long index) {
    return startTime + interval * index;
  }

  public static long convertDateStrToTimestamp(String dateStr) {
    DateTime dateTime = new DateTime(dateStr);
    return dateTime.getMillis();
  }

  /**
   * 按天拆分线程
   */
  public static int timeRange(long startTime, long endTime) {
    return (int) Math.ceil((float) (endTime - startTime) / Constants.TIME_DAY);
  }

  public static void main(String[] args) {
    long startTime = convertDateStrToTimestamp("2006-01-26T13:30:00+08:00");
    long endTime = convertDateStrToTimestamp("2006-01-27T13:30:01+08:00");
    System.out.println(timeRange(startTime, endTime));
  }

}
