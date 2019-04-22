package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import static cn.edu.tsinghua.iotdb.kairosdb.util.Preconditions.checkNotNullOrEmpty;

import cn.edu.tsinghua.iotdb.kairosdb.query.QueryException;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import java.util.LinkedList;
import java.util.List;

public class QueryAggregatorFilter extends QueryAggregator {

  private FilterOperandType operandType;
  private double threshold;

  QueryAggregatorFilter() {
    super(QueryAggregatorType.FILTER);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) throws QueryException {
    List<MetricValueResult> valueResults = result.getResults();

    for (MetricValueResult valueResult : valueResults) {

      if (valueResult.isTextType()) {
        continue;
      }

      List<QueryDataPoint> list = new LinkedList<>();

      for (QueryDataPoint point : valueResult.getDatapoints()) {
        switch (operandType) {
          case GT:
            if (point.getAsDouble() > threshold) {
              list.add(point);
            }
            break;
          case GTE:
            if (point.getAsDouble() >= threshold) {
              list.add(point);
            }
            break;
          case LT:
            if (point.getAsDouble() < threshold) {
              list.add(point);
            }
            break;
          case LTE:
            if (point.getAsDouble() <= threshold) {
              list.add(point);
            }
            break;
          case EQUAL:
            if (point.getAsDouble() == threshold) {
              list.add(point);
            }
            break;
          default:
            throw new QueryException("Among filter aggregator, "
                + "threshold must be one of [gt, gte, lt, lte, equal]");
        }
      }

      valueResult.setValues(list);
    }

    return result;
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
