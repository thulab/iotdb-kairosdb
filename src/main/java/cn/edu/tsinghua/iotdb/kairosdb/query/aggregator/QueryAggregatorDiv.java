package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.query.QueryException;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import java.util.List;

public class QueryAggregatorDiv extends QueryAggregator {

  private double divisor;

  QueryAggregatorDiv() {
    super(QueryAggregatorType.DIV);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) throws QueryException {

    if (getDivisor() == 0) {
      throw new QueryException("Among div aggregator, divisor can't be zero");
    }

    List<MetricValueResult> valueResults = result.getResults();

    for (MetricValueResult valueResult : valueResults) {

      if (valueResult.isTextType()) {
        continue;
      }

      valueResult.getDatapoints().forEach(point -> point.dividedBy(getDivisor()));

    }

    return result;
  }

  private double getDivisor() {
    return divisor;
  }

  void setDivisor(double divisor) {
    this.divisor = divisor;
  }

}
