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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


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
        StringBuilder createextsql = null;
        StringBuilder createkudusql = null;
        HdfsOp dfsop = new HdfsOp();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        String sql;
        sql = "select table_name from user_tables";
        List<String> tabList = new ArrayList<String>();
        ConvertCreateTableSql convertsql = new ConvertCreateTableSql();
        ImpalaTableOp iitc = new ImpalaTableOp();
        IsTablePk ispk = new IsTablePk();
        GetTextLines getlines = new GetTextLines();
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
        for (String aTabList : tabList) {
            //创建hdfs存储csv文件目录
            Date startdate = new Date();
            String tablename = aTabList.toLowerCase();
            String win_local_filename = TableToCSV.local_path + "\\" + aTabList + ".csv";
            String linux_local_filename = TableToCSV.local_path + "/" + aTabList + ".csv";
            //获取文件行数
            int filelines = getlines.getTextLines(linux_local_filename);
            String hdfs_filename = HdfsOp.HDFS_UPLOAD_PATH + "/" + tablename + "/" + aTabList + ".csv";
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
                createextsql = convertsql.genCreateExtTableSql(aTabList, HdfsOp.HDFS_UPLOAD_PATH + "/" + tablename);
            } catch (Exception e) {
                e.printStackTrace();
            }

            boolean issucceed = iitc.createTable(createextsql == null ? null : createextsql.toString());
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

            boolean iscreatekudusucceed = iitc.createTable(createkudusql == null ? null : createkudusql.toString());
            if (iscreatekudusucceed) {
                logger.info("Crate kudu table[" + tablename + "] succeed");
                //删除外表
                String exttablename = "ext" + tablename.toLowerCase();
                boolean isdropexttable = iitc.dropExtTable(exttablename);
                logger.info("Drop impala EXT[" + exttablename + " ] succeed");

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

            Date stopdate = new Date();
            //获取时间差
            long interval = (stopdate.getTime() - startdate.getTime());
            logger.info("Load table [" + tablename.toLowerCase() + "] data to kudu succeed");
            StringBuilder dsb = new StringBuilder();
            dsb.append(df.format(new Date()));
            dsb.append("  ");
            dsb.append("Load table [").append(tablename.toLowerCase()).append("] data to kudu succeed");
            dsb.append(",");
            dsb.append("total rows:");
            dsb.append(filelines).append(",cost time:").append(interval).append(" s");
            System.out.println(dsb);
        }
    }
}
