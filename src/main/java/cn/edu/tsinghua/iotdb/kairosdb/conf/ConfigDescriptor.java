package cn.edu.tsinghua.iotdb.kairosdb.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class ConfigDescriptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigDescriptor.class);

    private Config config;

    private static class ConfigDescriptorHolder {
        private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
    }

    private ConfigDescriptor() {
        config = new Config();
        loadProps();
    }

    public static final ConfigDescriptor getInstance() {
        return ConfigDescriptorHolder.INSTANCE;
    }

    public Config getConfig() {
        return config;
    }

    private void loadProps() {
        String url = System.getProperty(Constants.BENCHMARK_CONF, null);
        if (url != null) {
            InputStream inputStream = null;
            try {
                inputStream = new FileInputStream(new File(url));
            } catch (FileNotFoundException e) {
                LOGGER.warn("Fail to find config file {}", url);
                return;
            }
            Properties properties = new Properties();
            try {
                properties.load(inputStream);
                config.host = properties.getProperty("HOST", "no host");
                config.port = properties.getProperty("PORT", "no port");
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    LOGGER.error("Fail to close config file input stream", e);
                }
            }
        } else {
            LOGGER.warn("{} No config file path, use default config", Constants.CONSOLE_PREFIX);
        }
    }
}
