package kairosdb.export.csv;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransToTsfile {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransToTsfile.class);

  public static boolean isNumber(String s) {
    checkNotNull(s);

    if (s.isEmpty()) {
      return false;
    }

    int start = 0;
    char firstChar = s.charAt(0);
    if (firstChar == '+' || firstChar == '-' || firstChar == '.') {
      start = 1;
      if (s.length() == 1) {
        return false;
      }
    }
    int pointCount = 0;
    for (int i = start; i < s.length(); i++) {
      char c = s.charAt(i);
      if (pointCount > 1) {
        return false;
      }
      if (c == '.') {
        pointCount++;
      }
      if (!Character.isDigit(c) && c != '.') {
        return false;
      }
    }

    //noinspection RedundantIfStatement
    if (s.charAt(s.length() - 1) == '.') {
      return false; // can't have trailing period
    }

    return true;
  }

  public static void transToTsfile(String dirPath, String tsPath) {
    try {
      File f = new File(tsPath);
      if (!f.getParentFile().exists()) {
        f.getParentFile().mkdirs();
      }

      try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
        if (!new File(dirPath).exists()) {
          return;
        }
        File[] csvFiles = new File(dirPath).listFiles();
        for (File csvFile : csvFiles) {
          try (BufferedReader csvReader = new BufferedReader(new FileReader(csvFile))) {
            String header = csvReader.readLine();
            String[] sensorFull = Arrays
                .copyOfRange(header.split(","), 1, header.split(",").length);
            ArrayList<String> sensorList = new ArrayList<>(Arrays.asList(sensorFull));
            String device = sensorList.get(0).substring(0, sensorList.get(0).lastIndexOf('.'));
            for (int i = 0; i < sensorList.size(); i++) {
              sensorList.set(i, sensorList.get(i).replace(device, "").substring(1));
            }
            List<TSDataType> tsDataTypes = Arrays.asList(new TSDataType[sensorList.size()]);
//            String intRegex = "[0-9]+";
//            String floatRegex = "[0-9]+.[0-9]+";
            String line;
            while ((line = csvReader.readLine()) != null) {
              // construct TSRecord
              long time = Long.parseLong(line.split(",")[0]);
              TSRecord tsRecord = new TSRecord(time, device);
              String[] points = Arrays.copyOfRange(line.split(","), 1, line.split(",").length);
              for (int i = 0; i < points.length; i++) {
//                if (points[i].matches(intRegex)) {
//                  if (tsDataTypes.get(i) == null) {
//                    tsDataTypes.set(i, TSDataType.INT64);
//                    try {
//                      tsFileWriter.addMeasurement(new MeasurementSchema(sensorList.get(i),
//                          TSDataType.INT64, TSEncoding.TS_2DIFF));
//                    } catch (Exception e) {
//                      //
//                    }
//                    DataPoint intPoint = new LongDataPoint(sensorList.get(i),
//                        Long.parseLong(points[i]));
//                    tsRecord.addTuple(intPoint);
//                  } else {
//                    DataPoint intPoint = new LongDataPoint(sensorList.get(i),
//                        Long.parseLong(points[i]));
//                    tsRecord.addTuple(intPoint);
//                  }
//                } else if (points[i].matches(intRegex) || points[i].matches(floatRegex)) {
                if (isNumber(points[i])) {
                  if (tsDataTypes.get(i) == null) {
                    tsDataTypes.set(i, TSDataType.FLOAT);
                    try {
                      tsFileWriter.addMeasurement(new MeasurementSchema(sensorList.get(i),
                          TSDataType.FLOAT, TSEncoding.GORILLA));
                    } catch (Exception e) {
                      //
                    }
                    DataPoint floatPoint = new FloatDataPoint(sensorList.get(i),
                        Float.parseFloat(points[i]));
                    tsRecord.addTuple(floatPoint);
                  } else {
                    DataPoint floatPoint = new FloatDataPoint(sensorList.get(i),
                        Float.parseFloat(points[i]));
                    tsRecord.addTuple(floatPoint);
                  }
                } else {
                  if (!points[i].equals("")) {
                    if (tsDataTypes.get(i) == null) {
                      tsDataTypes.set(i, TSDataType.TEXT);
                      try {
                        tsFileWriter.addMeasurement(new MeasurementSchema(sensorList.get(i),
                            TSDataType.TEXT, TSEncoding.PLAIN));
                      } catch (Exception e) {
                        //
                      }
                      DataPoint textPoint = new StringDataPoint(sensorList.get(i),
                          Binary.valueOf(points[i]));
                      tsRecord.addTuple(textPoint);
                    } else {
                      DataPoint textPoint = new StringDataPoint(sensorList.get(i),
                          Binary.valueOf(points[i]));
                      tsRecord.addTuple(textPoint);
                    }
                  }
                }
              }
              try {
                tsFileWriter.write(tsRecord);
              } catch (Exception e) {
                LOGGER.error("write record error: {}, error csv: {}", e.getMessage(),
                    csvFile.getAbsolutePath(), e);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("error occurs when generate tsfile", e);
    }

  }

  public static void main(String[] args) {
    transToTsfile("/Users/tianyu/git_project/iotdb-kairosdb/tools/export/res/csv",
        "/Users/tianyu/git_project/iotdb-kairosdb/tools/export/res/test1.tsfile");
  }

}
