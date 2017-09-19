package com.com.util;

import com.export.data.DBConnections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by Administrator on 2017/9/18.
 * 判断某个表是否存在主键
 */
public class IsTablePk {
    private static final Logger logger = LoggerFactory.getLogger(IsTablePk.class);

    /*
    *@参数 tablename 表名
    *@参数 username表所属owner
     */
    public boolean isContainPk(String tablename, String username) {
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pre = null;
        ResultSet result = null;
        Boolean Flag = null;
        try {
            conn = new DBConnections(DBConnections.URL, DBConnections.USERNAME, DBConnections.PASSWORD).getConn();
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            String sql = "select  col.column_name pk from user_constraints con,user_cons_columns col where con.constraint_name=col.constraint_name  and con.constraint_type='P'  and con.owner='" + username.toUpperCase() + "' and col.table_name='" + tablename.toUpperCase() + "'";
            stmt = conn.createStatement();
            pre = conn.prepareStatement(sql);// 实例化预编译语句
            result = pre.executeQuery();// 执行查询，注意括号中不需要再加参数
            if (result.next()) {
                Flag = true;
            } else {
                Flag = false;
                logger.error("Table[" + tablename.toLowerCase() + "] has not primary key");
            }
        } catch (Exception e) {
            e.printStackTrace();
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
            return Flag;
        }
    }
}
