package com.com.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Administrator on 2017/9/14.
 */
//关于Properties类常用的操作
 /*
    读取配置文件
     */
public class EDKProperties {
    private static final Logger logger = LoggerFactory.getLogger(EDKProperties.class);

    //根据Key读取Value
    public static String loadProperties(String key) {
        try {
            InputStream inStream = new FileInputStream(new File("conf/edk.properties"));
            Properties prop = new Properties();
            prop.load(inStream);
            prop.getProperty(key);
            return prop.getProperty(key);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Read edk.properties file faild" + e.getMessage(), e);
            return null;
        }
    }

    public static String loadPk(String key) {
        try {
            InputStream inStream = new FileInputStream(new File("conf/pk.properties"));
            Properties prop = new Properties();
            prop.load(inStream);
            prop.getProperty(key);
            return prop.getProperty(key);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Read pk.properties file faild" + e.getMessage(), e);
            return null;
        }
    }
}