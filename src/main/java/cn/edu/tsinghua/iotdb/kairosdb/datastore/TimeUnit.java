/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package cn.edu.tsinghua.iotdb.kairosdb.datastore;


import static cn.edu.tsinghua.iotdb.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public enum TimeUnit {
  MILLISECONDS,
  SECONDS,
  MINUTES,
  HOURS,
  DAYS,
  WEEKS,
  MONTHS,
  YEARS;

  public static TimeUnit from(String value) {
    checkNotNullOrEmpty(value);
    for (TimeUnit unit : values()) {
      if (unit.toString().equalsIgnoreCase(value)) {
        return unit;
      }
    }

    throw new IllegalArgumentException("No enum constant for " + value);
  }

  public static long getUnitTime(TimeUnit unit) {
    if (unit == null) {
      return 0;
    }
    switch (unit) {
      case MILLISECONDS:
        return 1L;
      case SECONDS:
        return 1000L;
      case MINUTES:
        return 60000L;
      case HOURS:
        return 3600000L;
      case DAYS:
        return 86400000L;
      case WEEKS:
        return 604800000L;
      case MONTHS:
        return 2419200000L;
      case YEARS:
        return 29030400000L;
      default:
        return 0;
    }
  }

  public static java.util.concurrent.TimeUnit getUnitTimeInRollUp(TimeUnit unit) {
    if (unit == null) {
      return java.util.concurrent.TimeUnit.SECONDS;
    }
    switch (unit) {
      case MILLISECONDS:
        return java.util.concurrent.TimeUnit.MILLISECONDS;
      case SECONDS:
        return java.util.concurrent.TimeUnit.SECONDS;
      case MINUTES:
        return java.util.concurrent.TimeUnit.MINUTES;
      case HOURS:
        return java.util.concurrent.TimeUnit.HOURS;
      case DAYS:
        return java.util.concurrent.TimeUnit.DAYS;
      case WEEKS:
        return java.util.concurrent.TimeUnit.DAYS;
      case MONTHS:
        return java.util.concurrent.TimeUnit.DAYS;
      case YEARS:
        return java.util.concurrent.TimeUnit.DAYS;
      default:
        return java.util.concurrent.TimeUnit.SECONDS;
    }
  }

  public static boolean contains(String value) {
    for (TimeUnit unit : values()) {
      if (unit.name().equalsIgnoreCase(value)) {
        return true;
      }
    }

    return false;
  }

  public static String toValueNames() {
    StringBuilder builder = new StringBuilder();
    boolean firstTime = true;
    for (TimeUnit timeUnit : values()) {
      if (!firstTime) {
        builder.append(',');
      }
      builder.append(timeUnit.name());
      firstTime = false;
    }
    return builder.toString();
  }
}

