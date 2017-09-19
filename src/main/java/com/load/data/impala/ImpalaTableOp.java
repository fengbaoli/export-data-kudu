package com.load.data.impala;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;

/**
 * Created by Administrator on 2017/9/18.
 */
public class ImpalaTableOp {
    final private Logger logger = LoggerFactory.getLogger(ImpalaTableOp.class);

    public boolean createTable(String sql) {
        boolean flag = false;
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = new ImpalaDBConnect(ImpalaDBConnect.URL).getConn();
            stmt = conn.createStatement();
            stmt.execute(sql);
            conn.setAutoCommit(false);
            flag = true;
            logger.info("Crate impala table");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Crate impala table error " + e.getMessage());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                    logger.info("close stmt!");
                }
                if (conn != null) {
                    conn.close();
                    logger.info("conn stmt!");
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("close conn failed!");
            }
            return flag;
        }
    }

    public boolean dropExtTable(String tablename) {
        boolean flag = false;
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = new ImpalaDBConnect(ImpalaDBConnect.URL).getConn();
            stmt = conn.createStatement();
            String sql = "drop table " + tablename;
            stmt.execute(sql);
            conn.setAutoCommit(false);
            flag = true;
            logger.info("Drop impala ext table");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Drop impala ext table error " + e.getMessage());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                    logger.info("close stmt!");
                }
                if (conn != null) {
                    conn.close();
                    logger.info("conn stmt!");
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("close conn failed!");
            }
            return flag;
        }
    }


}
