package kairosdb.valid.ana.conf;

import java.util.ArrayList;
import java.util.List;

public class Config {

  public String KAIROS_URL = "http://127.0.0.1:8080";
  public String IKR_URL = "http://127.0.0.1:6666";
  public List<String> METRICS = new ArrayList<String>();
  public String TAG = "";
  public String START_TIME_ = "";
  public String END_TIME_ = "";

}
