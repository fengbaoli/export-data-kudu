package com.export.data;

import com.com.util.EDKProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Administrator on 2017/9/15.
 * 生成impala 外表及kudu表sql
 */
public class ConvertCreateTableSql {
    private static final Logger logger = LoggerFactory.getLogger(ConvertCreateTableSql.class);
    private Connection conn = null;
    private Statement stmt = null;
    private PreparedStatement pre = null;
    private ResultSet result = null;

    /*
    *@参数 tablename 表名称
    *@参数 hdfspath csv上传到hdfs的路径
     */
    @SuppressWarnings("ReturnInsideFinallyBlock")
    public ArrayList<String> genCreateExtTableSql(ArrayList<String> tablenames, String HDFS_UPLOAD_PATH) {
        ArrayList<String> extsqllists = new ArrayList<String>();
        //先查询出所有表的列，生成MAP
        HashMap<String, String> alltabrows = new HashMap();
        try {
            conn = new DBConnections(DBConnections.URL, DBConnections.USERNAME, DBConnections.PASSWORD).getConn();
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            for (String tablename1 : tablenames) {
                StringBuilder sb = new StringBuilder();
                String tablename = tablename1;
                String sql = "select COLUMN_NAME from user_tab_columns  where TABLE_NAME='" + tablename.toUpperCase() + "'";
                pre = conn.prepareStatement(sql);// 实例化预编译语句
                result = pre.executeQuery();// 执行查询，注意括号中不需要再加参数
                while (result.next()) {
                    sb.append(result.getString("COLUMN_NAME")).append(" String,");
                }
                sb = sb.deleteCharAt(sb.length() - 1);
                alltabrows.put(tablename, sb.toString());
            }
        } catch (Exception e) {
            logger.error("when select all tables columns connect oracle failed" + e.getMessage());
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
        //生成sql
        for (String tablename : tablenames) {
            String tablerows = alltabrows.get(tablename);
            String hdfspath = HDFS_UPLOAD_PATH + "/" + tablename;
            String extsql = "CREATE EXTERNAL TABLE IF NOT EXISTS ext" +
                    tablename.toLowerCase() +
                    "(" +
                    tablerows +
                    ") ROW FORMAT DELIMITED\n" +
                    "FIELDS TERMINATED BY ','  " +
                    "LOCATION '" + hdfspath + "' TBLPROPERTIES ('skip.header.line.count'='1')";
            extsqllists.add(extsql);
        }
        return extsqllists;
    }


    /*
*@参数 tablename 表名称
*@参数 username 表所属owner
 */
    public ArrayList<String> genCreateKuduTableSql(String username, ArrayList<String> tablenames) {
        ArrayList<String> kudusqllists = new ArrayList<String>();
        //先查询主键，生成MAP
        HashMap alltabpks = new HashMap();
        try {
            conn = new DBConnections(DBConnections.URL, DBConnections.USERNAME, DBConnections.PASSWORD).getConn();
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            for (String tablename1 : tablenames) {
                StringBuilder sb = new StringBuilder();
                String tablename = tablename1;
                String sql;
                sql = "select  col.column_name pk from user_constraints con,user_cons_columns col where con.constraint_name=col.constraint_name  and con.constraint_type='P'  and con.owner='" + username.toUpperCase() + "' and col.table_name='" + tablename.toUpperCase() + "'";
                pre = conn.prepareStatement(sql);// 实例化预编译语句
                result = pre.executeQuery();// 执行查询，注意括号中不需要再加参数
                while (result.next()) {
                    sb.append(result.getString("pk").toLowerCase());
                    sb.append(",");
                }
                sb = sb.deleteCharAt(sb.length() - 1);
                alltabpks.put(tablename, sb.toString());
            }
        } catch (Exception e) {
            logger.error("when select all tables pk connect oracle failed" + e.getMessage());
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
        //查询所有表所有列,生成MAP
        HashMap<String, String> alltabrows = new HashMap();
        try {
            conn = new DBConnections(DBConnections.URL, DBConnections.USERNAME, DBConnections.PASSWORD).getConn();
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            for (String tablename1 : tablenames) {
                StringBuilder sb = new StringBuilder();
                String tablename = tablename1;
                String sql = "select COLUMN_NAME from user_tab_columns  where TABLE_NAME='" + tablename.toUpperCase() + "'";
                pre = conn.prepareStatement(sql);// 实例化预编译语句
                result = pre.executeQuery();// 执行查询，注意括号中不需要再加参数
                while (result.next()) {
                    sb.append(result.getString("COLUMN_NAME")).append(" ,");
                }
                sb = sb.deleteCharAt(sb.length() - 1);
                alltabrows.put(tablename, sb.toString());
            }
        } catch (Exception e) {
            logger.error("when select all tables columns connect oracle failed" + e.getMessage());
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

        //生成sql
        for (String tablename : tablenames) {
            String tablepk = (String) alltabpks.get(tablename);
            String alltabrow = alltabrows.get(tablename);
            StringBuilder kudusql = new StringBuilder();
            kudusql.append("CREATE  TABLE  IF NOT EXISTS  ");
            kudusql.append(tablename.toLowerCase());
            kudusql.append(" PRIMARY KEY (");
            kudusql.append(tablepk);
            kudusql.append(")");
            //添加分区键
            String partionkey = EDKProperties.loadPk(tablename.toLowerCase());
            if (partionkey != null && !partionkey.equals("")) {
                kudusql.append(" PARTITION BY HASH ");
                kudusql.append("(");
                kudusql.append(partionkey);
                kudusql.append(")");
                kudusql.append(" PARTITIONS 8 ");
            } else {
                kudusql.append(" PARTITION BY HASH ");
                kudusql.append(" PARTITIONS 8 ");
            }
            kudusql.append(" STORED AS KUDU AS ");
            kudusql.append("SELECT ");
            kudusql.append(alltabrow);
            kudusql.append(" FROM ");
            kudusql.append("ext");
            kudusql.append(tablename.toLowerCase());
            kudusqllists.add(kudusql.toString());
        }
        return kudusqllists;

    }

}
