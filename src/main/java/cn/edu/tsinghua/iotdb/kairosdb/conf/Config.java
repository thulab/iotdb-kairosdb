package cn.edu.tsinghua.iotdb.kairosdb.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
    private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);

    public Config() { }

    public String host = "localhost";
    public String port = "6667";

}
