package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.DataPointsParser;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteWorker extends Thread {

  private final Gson gson;
  private static final Logger LOGGER = LoggerFactory.getLogger(DataPointsParser.class);

  public WriteWorker() {
    GsonBuilder builder = new GsonBuilder();
    gson = builder.disableHtmlEscaping().create();
  }

  @Override
  public void run() {

    while (true) {
      Reader reader = MessageQueue.getInstance().poll();
      if (reader != null) {
        LOGGER.info("reader is not null");
        DataPointsParser parser = new DataPointsParser(reader, gson);

        try {
          parser.parse();
        } catch (IOException e) {
          LOGGER.error("Write worker execute parser.parse() failed because ", e);
        }
      }
    }

  }

}
