package com.export.data;

import com.com.util.EDKProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by Administrator on 2017/9/15.
 * 生成impala 外表及kudu表sql
 */
public class ConvertCreateTableSql {
    private static final Logger logger = LoggerFactory.getLogger(ConvertCreateTableSql.class);
    Connection conn = null;
    Statement stmt = null;
    PreparedStatement pre = null;
    ResultSet result = null;

    /*
    *@参数 tablename 表名称
    *@参数 hdfspath csv上传到hdfs的路径
     */
    public StringBuffer genCreateExtTableSql(String tablename, String hdfspath) throws Exception {
        logger.info("Start generate table[" + tablename.toLowerCase() + "] create ext DML SQL");
        StringBuffer sb = new StringBuffer();
        sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS ext");
        sb.append(tablename.toLowerCase() + "(");
        try {
            conn = new DBConnections(DBConnections.URL, DBConnections.USERNAME, DBConnections.PASSWORD).getConn();
            conn.setAutoCommit(false);
            String sql = "select COLUMN_NAME from user_tab_columns  where TABLE_NAME='" + tablename.toUpperCase() + "'";
            stmt = conn.createStatement();
            pre = conn.prepareStatement(sql);// 实例化预编译语句
            result = pre.executeQuery();// 执行查询，注意括号中不需要再加参数
            while (result.next()) {
                sb.append(result.getString("COLUMN_NAME") + " String,");
            }
            sb = sb.deleteCharAt(sb.length() - 1);
            sb.append(") ROW FORMAT DELIMITED\n" +
                    "FIELDS TERMINATED BY ','  ");
            sb.append("LOCATION '" + hdfspath + "' TBLPROPERTIES ('skip.header.line.count'='1')");
            stmt.close();
            conn.close();
            logger.info("Generate table[" + tablename.toLowerCase() + "]  ext DML SQL succeed");
        } catch (Exception e) {
            logger.error("When generate table[" + tablename.toLowerCase() + "] create ext DML SQL,create jdbc failed");
            logger.error("" + e.getMessage());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return sb;
        }
    }

    /*
*@参数 tablename 表名称
*@参数 username 表所属owner
 */
    public StringBuffer genCreateKuduTableSql(String username, String tablename) {
        logger.info("Start generate table[" + tablename.toLowerCase() + "] create KUDU DML SQL");
        EDKProperties pro = new EDKProperties();
        StringBuffer sb = new StringBuffer();
        sb.append("CREATE  TABLE  IF NOT EXISTS  ");
        sb.append(tablename.toLowerCase());
        sb.append(" PRIMARY KEY (");
        try {
            conn = new DBConnections(DBConnections.URL, DBConnections.USERNAME, DBConnections.PASSWORD).getConn();
            conn.setAutoCommit(false);
            //查询表字段sql
            String sql = "select  col.column_name pk from user_constraints con,user_cons_columns col where con.constraint_name=col.constraint_name  and con.constraint_type='P'  and con.owner='" + username.toUpperCase() + "' and col.table_name='" + tablename.toUpperCase() + "'";
            stmt = conn.createStatement();
            pre = conn.prepareStatement(sql);// 实例化预编译语句
            result = pre.executeQuery();// 执行查询，注意括号中不需要再加参数
            while (result.next()) {
                sb.append(result.getString("pk").toLowerCase() + ",");
            }
            sb = sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            //添加分区键
            String partionkey = pro.loadPk(tablename.toLowerCase());
            if (partionkey != null && partionkey != "") {
                sb.append(" PARTITION BY HASH ");
                sb.append("(" + partionkey + ")");
                sb.append(" PARTITIONS 8 ");
            } else {
                sb.append(" PARTITION BY HASH ");
                sb.append(" PARTITIONS 8 ");
            }
            sb.append(" STORED AS KUDU AS ");
            sb.append("SELECT ");
            //生成查询外表语句
            String decsql = "select COLUMN_NAME from user_tab_columns  where TABLE_NAME='" + tablename.toUpperCase() + "'";
            pre = conn.prepareStatement(decsql);
            result = pre.executeQuery();// 执行查询，注意括号中不需要再加参数
            while (result.next()) {
                sb.append(result.getString("COLUMN_NAME").toLowerCase() + ",");
            }
            sb = sb.deleteCharAt(sb.length() - 1);
            sb.append(" FROM ");
            sb.append("ext" + tablename.toLowerCase());
            stmt.close();
            conn.close();
            logger.info("Generate table[" + tablename.toLowerCase() + "] create KUDU DML SQL succeed ");
        } catch (Exception e) {
            logger.error("When generate table[" + tablename.toLowerCase() + "] create kudu DML SQL,create jdbc failed");
            logger.error("" + e);
        } finally {
            try {
                if (result != null) {
                    result.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return sb;
    }
}
