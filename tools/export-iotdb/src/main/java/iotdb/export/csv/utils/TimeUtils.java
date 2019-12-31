package iotdb.export.csv.utils;

import java.text.SimpleDateFormat;
import org.joda.time.DateTime;

public class TimeUtils {

  static String df1 = "yyyy-MM-dd HH:mm:ss";

  public static long convertDateStrToTimestamp(String dateStr) {
    DateTime dateTime = new DateTime(dateStr);
    return dateTime.getMillis();
  }

  public static String timstamp2DateTime(Long timestamp) {
    return new SimpleDateFormat(df1).format(timestamp);
  }

  public static void main(String[] args) {
    System.out.println(convertDateStrToTimestamp("2006-01-26T13:30:00+08:00"));
    System.out.println(convertDateStrToTimestamp("2006-01-26T13:30:01+08:00"));
  }

}
