package com.load.data.impala;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;


public class ImpalaTableOp {
    final private Logger logger = LoggerFactory.getLogger(ImpalaTableOp.class);

    public void createTable(ArrayList<String> sqls) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = new ImpalaDBConnect(ImpalaDBConnect.URL).getConn();
            stmt = conn.createStatement();
            for (String sql : sqls) {
                boolean result = stmt.execute(sql);
                if (result) {
                    logger.error("Create impala table failed");
                    break;
                }
                conn.setAutoCommit(false);
                logger.info("Create impala table");
            }
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
        }
    }

    @SuppressWarnings("ReturnInsideFinallyBlock")
    public boolean dropExtTable(String tablename) {
        boolean flag = false;
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = new ImpalaDBConnect(ImpalaDBConnect.URL).getConn();
            stmt = conn.createStatement();
            String sql = new StringBuilder().append("drop table ").append(tablename).toString();
            stmt.execute(sql);
            conn.setAutoCommit(false);
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
            } finally {
                return false;
            }
        }
    }


}
