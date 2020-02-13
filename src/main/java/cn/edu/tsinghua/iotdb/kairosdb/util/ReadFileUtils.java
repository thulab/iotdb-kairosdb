package cn.edu.tsinghua.iotdb.kairosdb.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadFileUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadFileUtils.class);

  public static String readJsonFile(String fileName) {
    String jsonStr = "";

    File jsonFile = new File(fileName);
    try (Reader reader = new InputStreamReader(new FileInputStream(jsonFile),
        StandardCharsets.UTF_8)) {
      int ch = 0;
      StringBuilder sb = new StringBuilder();
      while ((ch = reader.read()) != -1) {
        sb.append((char) ch);
      }

      jsonStr = sb.toString();
      return jsonStr;
    } catch (IOException e) {
      LOGGER.error("Read json file failed because:", e);
      return null;
    }
  }
}
