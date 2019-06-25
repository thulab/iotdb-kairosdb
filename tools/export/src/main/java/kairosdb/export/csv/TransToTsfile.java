package kairosdb.export.csv;

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
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class TransToTsfile {

  public static void transToTsfile(String csvPath, String tsPath) {
    try {
      File f = new File(tsPath);

      try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
        try (BufferedReader csvReader = new BufferedReader(new FileReader(csvPath))) {
          String header = csvReader.readLine();
          String[] sensorFull = Arrays.copyOfRange(header.split(","), 1, header.split(",").length);
          ArrayList<String> sensorList = new ArrayList<>(Arrays.asList(sensorFull));
          String device = sensorList.get(1).substring(0, sensorList.get(1).lastIndexOf('.'));
          for (int i = 0; i < sensorList.size(); i++) {
            sensorList.set(i, sensorList.get(i).replace(device, "").substring(1));
          }
          List<TSDataType> tsDataTypes = Arrays.asList(new TSDataType[sensorList.size()]);
          String intRegex = "[0-9]+";
          String floatRegex = "[0-9]+.[0-9]+";
          String line;
          while ((line = csvReader.readLine()) != null) {
            // construct TSRecord
            long time = Long.parseLong(line.split(",")[0]);
            TSRecord tsRecord = new TSRecord(time, device);
            String[] points = Arrays.copyOfRange(line.split(","), 1, line.split(",").length);
            for (int i = 0; i < points.length; i++) {
              if (points[i].matches(intRegex)) {
                if (tsDataTypes.get(i) == null) {
                  tsDataTypes.set(i, TSDataType.INT32);
                  tsFileWriter.addMeasurement(new MeasurementSchema(sensorList.get(i),
                      TSDataType.INT32, TSEncoding.TS_2DIFF));
                  DataPoint intPoint = new IntDataPoint(sensorList.get(i),
                      Integer.parseInt(points[i]));
                  tsRecord.addTuple(intPoint);
                } else {
                  DataPoint intPoint = new IntDataPoint(sensorList.get(i),
                      Integer.parseInt(points[i]));
                  tsRecord.addTuple(intPoint);
                }
              } else if (points[i].matches(floatRegex)) {
                if (tsDataTypes.get(i) == null) {
                  tsDataTypes.set(i, TSDataType.DOUBLE);
                  tsFileWriter.addMeasurement(new MeasurementSchema(sensorList.get(i),
                      TSDataType.DOUBLE, TSEncoding.GORILLA));
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
                    tsFileWriter.addMeasurement(new MeasurementSchema(sensorList.get(i),
                        TSDataType.TEXT, TSEncoding.PLAIN));
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
            tsFileWriter.write(tsRecord);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public static void main(String[] args) {
    transToTsfile("/Users/tianyu/git_project/iotdb-kairosdb/tools/export/res/test.csv",
        "/Users/tianyu/git_project/iotdb-kairosdb/tools/export/res/test.tsfile");
  }

}
