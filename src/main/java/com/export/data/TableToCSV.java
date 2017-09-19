package com.export.data;

/**
 * Created by Administrator on 2017/9/12.
 */

import com.com.util.EDKProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class TableToCSV {
    private static final Logger logger = LoggerFactory.getLogger(TableToCSV.class);
    static EDKProperties pro = new EDKProperties();
    public static final String local_path = pro.loadProperties("local_path");
    public final String timezone = pro.loadProperties("timezone");

    //判断某列是datetime类型且包含时区
    public boolean isContains(String s) {
        return s.contains(timezone);
    }

    public List<String> startTableToCSV(List<String> tl) throws Exception {
        List<String> fileList = new ArrayList<String>();
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = new DBConnections(DBConnections.URL, DBConnections.USERNAME, DBConnections.PASSWORD).getConn();
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            for (String t : tl) {
                int count = 0;
                //String filename = local_path+"\\"+generateFilename(t);
                String filename = local_path + "/" + generateFilename(t);
                File file = createEmptyFile(filename);
                FileWriter fw = new FileWriter(file, true);
                //将所有数据查询出来
                String sql = "select * from " + t;
                ResultSet rs = stmt.executeQuery(sql);
                writeToFile(fw, rs, ++count);
                fw.close();
                fileList.add(file.getAbsolutePath());
            }
            stmt.close();
            conn.close();
        } catch (Exception e) {
            logger.error("Crate jdbc faild" + e.getMessage());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                logger.error("close session faild" + e.getMessage());
            } finally {
                logger.info("close session");
            }
        }
        return fileList;
    }

    //将文件数据库记录写入文件
    private void writeToFile(FileWriter fw, ResultSet rs, int count) throws Exception {

        try {
            ResultSetMetaData rd = rs.getMetaData();
            int fields = rd.getColumnCount();
            if (rd.getColumnName(fields).equals("RN")) {
                fields--;
            }
            if (count == 1) {
                for (int i = 1; i <= fields; i++) {
                    fw.write(rd.getColumnName(i));
                    if (i == fields)
                        fw.write("\n");
                    else
                        fw.write(",");
                }
                fw.flush();
            }
            writeToFile(fields, fw, rs);
        } catch (Exception e) {
            logger.error("write to file failed" + e.getMessage());
        }
    }

    //将数据记录写入文件
    private void writeToFile(int fields, FileWriter fw, ResultSet rs) throws Exception {
        try {
            while (rs.next()) {
                for (int i = 1; i <= fields; i++) {
                    String temp = rs.getString(i);
                    //对包含时区的列的值去掉时区
                    if (temp != null && temp != "") {
                        if (isContains(temp) == true) {
                            //TimeZone timeZoneNY = TimeZone.getTimeZone(timezone);
                            SimpleDateFormat myformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                            Date date = myformat.parse(temp);
                            myformat.setTimeZone(TimeZone.getTimeZone("GMT+20")); // 设置时区为GMT  +8为北京时间东八区
                            temp = myformat.format(date.getTime());
                        }
                    }
                    if (!rs.wasNull()) {
                        //这里将记录里面的特殊符号进行替换， 假定数据中不包含替换后的特殊字串
                        temp = temp.replaceAll(",", "&%&");
                        temp = temp.replaceAll("\n\r|\r|\n|\r\n", "&#&");
                        fw.write(temp);
                    }
                    if (i == fields)
                        fw.write("\r\n");
                    else
                        fw.write(",");
                }
                fw.flush();
            }
        } catch (Exception e) {

            logger.error("Flush data to  csv failed" + e.getMessage());
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
    }

    //创建一个空文件
    private File createEmptyFile(String filename) throws Exception {

        File file = new File(filename);
        try {
            if (file.exists()) {
                file.delete();
                file.createNewFile();
            } else {
                file.createNewFile();
            }
            logger.info("Crate local file:" + filename);
        } catch (IOException e) {
            logger.error("Create local file" + filename + " Error");
            logger.error(e.getMessage());
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        return file;
    }

    private String generateFilename(String t) {
        String filename = "";
        filename += t;
        filename += ".csv";
        return filename;
    }
}