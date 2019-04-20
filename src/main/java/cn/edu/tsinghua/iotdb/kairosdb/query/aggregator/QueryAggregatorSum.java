package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import java.util.LinkedList;
import java.util.List;

public class QueryAggregatorSum extends QueryAggregator
    implements QueryAggregatorSampling, QueryAggregatorAlignable {

  private Duration sampling;

  private QueryAggregatorAlign align;

  QueryAggregatorSum() {
    super(QueryAggregatorType.SUM);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) {

    List<MetricValueResult> valueResults = result.getResults();

    List<MetricValueResult> newValueResults = new LinkedList<>();

    for (MetricValueResult valueResult : valueResults) {

      MetricValueResult newValueResult = new MetricValueResult(valueResult.getName());

      switch (align) {
        case ALIGN_SAMPLING:
          break;
        case ALIGN_START_TIME:
          break;
        case ALIGN_END_TIME:
          break;
        default:
          break;
      }





      newValueResults.add(newValueResult);

    }

    result.setResults(newValueResults);

    return result;
  }

  @Override
  public void setSampling(Duration sampling) {
    this.sampling = sampling;
  }

  @Override
  public Duration getSampling() {
    return sampling;
  }

  @Override
  public void setAlign(QueryAggregatorAlign align) {
    this.align = align;
  }

  @Override
  public QueryAggregatorAlign getAlign() {
    return align;
  }

}
