package com.com.util;

import com.export.data.ConvertCreateTableSql;
import com.export.data.DBConnections;
import com.export.data.TableToCSV;
import com.load.data.hdfs.HdfsOp;
import com.load.data.impala.ImpalaTableOp;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/9/15.
 */
public class LoadDataMain {
    private static final Logger logger = LoggerFactory.getLogger(LoadDataMain.class);

    //配置log4j位置
    static {
        PropertyConfigurator.configure(System.getProperty("user.dir") + File.separator + "conf" + File.separator
                + "log4j.properties");
    }
    //导出用户表数据到csv文件

    public static void main(String args[]) throws IOException {

        Connection conn = null;
        Statement stmt = null;
        StringBuffer createextsql = null;
        StringBuffer createkudusql = null;
        HdfsOp dfsop = new HdfsOp();
        String sql = "select table_name from user_tables";
        List<String> tabList = new ArrayList<String>();
        ConvertCreateTableSql convertsql = new ConvertCreateTableSql();
        ImpalaTableOp iitc = new ImpalaTableOp();
        IsTablePk ispk = new IsTablePk();

        try {
            conn = new DBConnections(DBConnections.URL, DBConnections.USERNAME, DBConnections.PASSWORD).getConn();
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            int count = 0;
            while (rs.next()) {
                String table = rs.getString(1);
                //判断表是否存在主键
                if (ispk.isContainPk(table, DBConnections.USERNAME)) {
                    tabList.add(table);
                }
            }
            stmt.close();
            conn.close();
            //表生成csv文件
            TableToCSV tablecsv = new TableToCSV();
            tablecsv.startTableToCSV(tabList);
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
        }
        //上传csv文件至hdfs指定目录及创建表
        for (int i = 0; i < tabList.size(); i++) {
            //创建hdfs存储csv文件目录
            String tablename = tabList.get(i).toLowerCase();
            String win_local_filename = TableToCSV.local_path + "\\" + tabList.get(i) + ".csv";
            String linux_local_filename = TableToCSV.local_path + "/" + tabList.get(i) + ".csv";
            String hdfs_filename = HdfsOp.HDFS_UPLOAD_PATH + "/" + tablename + "/" + tabList.get(i) + ".csv";
            try {
                dfsop.makeDir(HdfsOp.HDFS_UPLOAD_PATH);
            } catch (Exception e) {
                e.printStackTrace();
            }
            //上传
            try {
                //windowns
                //dfsop.uploadFile(win_local_filename,hdfs_filename);
                //linux
                dfsop.uploadFile(linux_local_filename, hdfs_filename);
            } catch (Exception e) {
                e.printStackTrace();
            }

            //创建外表
            try {
                createextsql = convertsql.genCreateExtTableSql(tabList.get(i), HdfsOp.HDFS_UPLOAD_PATH + "/" + tablename);
            } catch (Exception e) {
                e.printStackTrace();
            }

            boolean issucceed = iitc.createTable(createextsql.toString());
            if (issucceed) {
                logger.info("Crate ext table[" + "ext" + tablename + "] succeed");
            } else {
                logger.error("Crate ext table[" + "ext" + tablename + "] failed");

            }

            //创建kudu表及insert 数据
            try {
                createkudusql = convertsql.genCreateKuduTableSql(DBConnections.USERNAME, tablename);
            } catch (Exception e) {
                e.printStackTrace();
            }

            boolean iscreatekudusucceed = iitc.createTable(createkudusql.toString());
            if (iscreatekudusucceed) {
                logger.info("Crate kudu table[" + tablename + "] succeed");
                //删除外表
                String exttablename = "ext" + tablename.toLowerCase();
                boolean isdropexttable = iitc.dropExtTable(exttablename);
                if (iscreatekudusucceed) {
                    logger.info("Drop impala EXT[" + exttablename + " ] succeed");
                } else {
                    logger.error("Drop impala EXT[" + exttablename + " ] failed");
                }

                //删除hdfs csv文件
                try {
                    dfsop.deleteFile(hdfs_filename);
                } catch (Exception e) {
                    logger.error("delete file[" + hdfs_filename + "] failed" + e.getMessage(), e);
                } finally {
                    logger.info("delete file[" + hdfs_filename + "] succeed");
                }
            } else {
                logger.error("Crate kudu table[" + tablename + "] failed");
            }
            logger.info("Load table [" + tablename.toLowerCase() + "] data to kudu succeed");
            System.out.println("Load table [" + tablename.toLowerCase() + "] data to kudu succeed");
        }
    }
}
