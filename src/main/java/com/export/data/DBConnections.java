package com.export.data;


import com.com.util.EDKProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

public class DBConnections {
    private static final Logger logger = LoggerFactory.getLogger(DBConnections.class);
    public static EDKProperties pro = new EDKProperties();
    public static String URL = pro.loadProperties("ora_url");
    public static String USERNAME = pro.loadProperties("ora_username");
    public static String PASSWORD = pro.loadProperties("ora_password");
    public String url;
    public String username;
    public String password;

    public DBConnections(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public Connection getConn() throws Exception {
        Connection conn = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(url, username, password);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            logger.error("Jdbc:" + url + "connect failed " + e.getMessage());
        } finally {
            logger.info("Conect oracle:" + url + " succeed");
        }
        return conn;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}