package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import static cn.edu.tsinghua.iotdb.kairosdb.util.Preconditions.checkNotNullOrEmpty;

import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;

public class QueryAggregatorFilter extends QueryAggregator {

  private FilterOperandType operandType;
  private double threshold;

  protected QueryAggregatorFilter() {
    super(QueryAggregatorType.FILTER);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) {
    return null;
  }

  public void setOperandType(String typeStr) {
    this.operandType = FilterOperandType.fromString(typeStr);
  }

  public void setThreshold(double threshold) {
    this.threshold = threshold;
  }

  public double getThreshold() {
    return threshold;
  }

  private enum FilterOperandType {
    EQUAL, LT, LTE, GT, GTE;

    static FilterOperandType fromString(String typeStr) {
      checkNotNullOrEmpty(typeStr);
      for (FilterOperandType type : values()) {
        if (type.toString().equalsIgnoreCase(typeStr)) {
          return type;
        }
      }

      throw new IllegalArgumentException("No enum constant for " + typeStr);
    }
  }
}
