package cn.edu.tsinghua.iotdb.kairosdb.datastore.disruptor;

import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.DataPointsParser;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.lmax.disruptor.EventHandler;
import java.io.StringReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringEventHandler implements EventHandler<StringEvent> {

  private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
  private static final Logger LOGGER = LoggerFactory.getLogger(DataPointsParser.class);

  @Override
  public void onEvent(StringEvent stringEvent, long sequence, boolean endOfBatch) {

    try {
      String json = stringEvent.getValue();
      if (json != null && json.length() > 1) {
        StringReader stringReader = new StringReader(json);
        DataPointsParser parser = new DataPointsParser(stringReader, gson);
        parser.parse();
      }
    } catch (Exception e) {
      LOGGER.error("StringEventHandler failed to handle event because ", e);
    }
  }
}
