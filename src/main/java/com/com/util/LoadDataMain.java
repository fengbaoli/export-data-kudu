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
        ArrayList<String> createextsql;
        ArrayList<String> createkudusql;
        HdfsOp dfsop = new HdfsOp();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        String sql;
        sql = "select table_name from user_tables";
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
            System.out.println(df.format(new Date()) + " Load all table data to csv succeed\n");
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
/*
创建hdfs上传路径
 */
        try {
            dfsop.makeDir(HdfsOp.HDFS_UPLOAD_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }

/*
上传csv文件到hdfs
 */
        ArrayList<String> linux_local_filenames = new ArrayList<String>();
        ArrayList<String> hdfs_filenames = new ArrayList<String>();
        ArrayList<String> tablenames = new ArrayList<String>();
        for (String aTabList : tabList) {
            //创建hdfs存储csv文件目录
            Date startdate = new Date();
            String tablename = aTabList.toLowerCase();
            tablenames.add(tablename);
            String win_local_filename = TableToCSV.local_path + "\\" + aTabList + ".csv";
            String linux_local_filename = TableToCSV.local_path + "/" + aTabList + ".csv";
            linux_local_filenames.add(linux_local_filename);
            //获取文件行数
            String hdfs_filename = HdfsOp.HDFS_UPLOAD_PATH + "/" + tablename + "/" + aTabList + ".csv";
            hdfs_filenames.add(hdfs_filename);
        }

        //上传
        try {
            //windowns
            //dfsop.uploadFile(win_local_filename,hdfs_filename);
            //linux
            dfsop.uploadFile(linux_local_filenames, hdfs_filenames);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //创建外表
        createextsql = convertsql.genCreateExtTableSql(tablenames, HdfsOp.HDFS_UPLOAD_PATH);
        iitc.createTable(createextsql);
        System.out.println(df.format(new Date()) + " create all ext tables succeed\n");
        //创建kudu表及insert 数据
        createkudusql = convertsql.genCreateKuduTableSql(DBConnections.USERNAME, tablenames);
        iitc.createTable(createkudusql);
        System.out.println(df.format(new Date()) + " create all kudu tables succeed\n");
        //删除外表

        for (String tablename : tablenames) {
            String exttablename = "ext" + tablename;
            boolean isdroped = iitc.dropExtTable(exttablename);
            if (isdroped) {
                logger.info("drop table [" + exttablename + "] succeed");
                System.out.println(df.format(new Date()) + " drop table [" + exttablename + "] succeed\n");
            } else {
                System.out.println(df.format(new Date()) + " drop table [" + exttablename + "] failed\n");
                logger.error("drop table [" + exttablename + "] failed");
            }
        }
        //删除csv文件
        for (int i = 0; i < hdfs_filenames.size(); i++) {


            try {
                dfsop.deleteFile(hdfs_filenames);
                System.out.println(df.format(new Date()) + " delete hdfs file [" + hdfs_filenames + "] succeed\n");
            } catch (Exception e) {
                logger.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
