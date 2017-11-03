package com.ccfsoft.bigdata.utils;

import java.io.IOException;
import java.util.Properties;

/**
 * 配置文件读取类
 */
public class PropertyConstants {
    private static Properties properties;

    private static void setProperty(){
        if (properties==null) {
            properties = new Properties();
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try {
                properties.load(loader.getResourceAsStream("application.properties"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getPropertiesKey(String key){
        if (properties==null) {
            setProperty();
        }
        return properties.getProperty(key, "default");
    }

    public static Properties getProperties() {
        if (properties==null) {
            setProperty();
        }
        return properties;
    }
}
