package com.load.data.impala;

import com.com.util.EDKProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by Administrator on 2017/9/14.
 */
public class ImpalaDBConnect {
    private static final Logger logger = LoggerFactory.getLogger(ImpalaDBConnect.class);
    static EDKProperties pro = new EDKProperties();
    static String impala_url = pro.loadProperties("impala_url");
    static String databasename = pro.loadProperties("impala_database");
    static String URL = impala_url + "/" + databasename;
    private String url;


    public ImpalaDBConnect(String url) {
        this.url = url;
    }

    public Connection getConn() throws Exception {
        Connection conn = null;
        try {
            Class.forName("com.cloudera.impala.jdbc41.Driver");
            conn = DriverManager.getConnection(url);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            logger.error("Connect impala error" + e.getMessage());
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        return conn;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
