package com.kafka.crawl.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LoadProperties {
    private static LoadProperties loadProperties;
    private static Properties properties = new Properties();
    private static String properties_path = System.getProperty("user.dir") + File.separator + "resources" + File.separator + "config.properties";

    public static LoadProperties getInstance() {
        if (loadProperties == null) {
            loadProperties = new LoadProperties();
        }
        return loadProperties;
    }

    private LoadProperties() {
        try (InputStream input = new FileInputStream(properties_path)) {
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public Properties getProperties() {
        return properties;
    }
}
