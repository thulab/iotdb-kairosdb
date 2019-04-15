package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import static cn.edu.tsinghua.iotdb.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public enum QueryAggregatorType {

  AVG,
  DEV,
  COUNT,
  FIRST,
  LAST,
  MAX,
  MIN,
  PERCENTILE,
  SUM,
  DIFF,
  DIV,
  RATE,
  SAMPLER,
  SAVE_AS,
  FILTER;

  public static QueryAggregatorType fromString(String typeStr) {
    checkNotNullOrEmpty(typeStr);
    for (QueryAggregatorType type : values()) {
      if (type.toString().equalsIgnoreCase(typeStr)) {
        return type;
      }
    }

    throw new IllegalArgumentException("No enum constant for " + typeStr);
  }

}
