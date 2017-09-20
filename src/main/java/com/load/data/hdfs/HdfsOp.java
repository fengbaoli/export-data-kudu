package com.load.data.hdfs;

import com.com.util.EDKProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class HdfsOp {
    static final String HA_NAMENODES_VAL_KEY = "dfs.ha.namenodes." + DFS_NAMESERVERS;
    static final String RPC_ADDRESS1_VAL_KEY = "dfs.namenode.rpc-address." + DFS_NAMESERVERS + "." + NAMENODES.split(",")[0];
    static final String RPC_ADDRESS2_VAL_KEY = "dfs.namenode.rpc-address." + DFS_NAMESERVERS + "." + NAMENODES.split(",")[1];
    private static final Logger logger = LoggerFactory.getLogger(HdfsOp.class);
    static EDKProperties pro = new EDKProperties();
    static final String DFS_NAMESERVERS = pro.loadProperties("dfs.nameservices");
    static final String NAMENODES = pro.loadProperties(HA_NAMENODES_VAL_KEY);
    static final String FS_DEFAULTFS = pro.loadProperties("fs.defaultFS");
    public static final String HDFS_UPLOAD_PATH = pro.loadProperties("hdfs_path");
    public static final String hdfssuperuser = pro.loadProperties("hdfssuperuser");
    static Configuration conf = new Configuration(true);
    static {
        //指定hadoop fs的地址
        conf.set("fs.defaultFS", FS_DEFAULTFS);
        conf.set("dfs.nameservices", DFS_NAMESERVERS);
        conf.set(HA_NAMENODES_VAL_KEY, NAMENODES);
        conf.set(RPC_ADDRESS1_VAL_KEY, pro.loadProperties(RPC_ADDRESS1_VAL_KEY));
        conf.set(RPC_ADDRESS2_VAL_KEY, pro.loadProperties(RPC_ADDRESS2_VAL_KEY));
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    }

    public void makeDir(String newpath) throws IOException, InterruptedException {
        logger.info("Create hdfs dir:" + newpath);
        try {
            FileSystem fs = FileSystem.get(URI.create(FS_DEFAULTFS), conf, hdfssuperuser);
            Path path = new Path(newpath);
            fs.mkdirs(path);
            fs.close();

        } catch (Exception e) {
            logger.error("Create hdfs dir " + newpath + " error" + e.getMessage() + e);
        }

    }

    public void uploadFile(ArrayList<String> uploadfiles, ArrayList<String> unloadpaths) throws Exception {
        try {
            FileSystem fs = FileSystem.get(URI.create(FS_DEFAULTFS), conf, hdfssuperuser);
            for (int i = 0; i < uploadfiles.size(); i++) {
                String uploadfile = uploadfiles.get(i);
                String unloadpath = unloadpaths.get(i);
                logger.info("upload local file:" + uploadfile + " to hdfs:" + unloadpath);
                //要上传的源文件所在路径
                Path src = new Path(uploadfile);
                //hadoop文件系统的跟目录
                Path dst = new Path(unloadpath);
                //将源文件copy到hadoop文件系统
                //是否删除本地文件，覆盖hdfs文件
                //fs.copyFromLocalFile(false,true,src, dst);
                fs.copyFromLocalFile(true, true, src, dst);
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                System.out.println(df.format(new Date()) + " upload csvfile[" + uploadfile + "] to hdfs succeed\n");
            }
            fs.close();
        } catch (Exception e) {
            logger.error("Connect hdfs faild " + e.getMessage());
        }
    }

    public void deleteFile(ArrayList<String> fileNames) throws IOException, InterruptedException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        try {
            FileSystem fs = FileSystem.get(URI.create(FS_DEFAULTFS != null ? FS_DEFAULTFS : null), conf, hdfssuperuser);
            for (int i = 0; i < fileNames.size(); i++) {
                String fileName = fileNames.get(i);
                Path f = new Path(fileName);
                boolean isExists = fs.exists(f);
                if (isExists) { //if exists, delete
                    boolean isDel = fs.delete(f, true);
                    if (isDel) {
                        logger.info("Deelte File [" + fileName + "] succeed");
                        System.out.println(df.format(new Date()) + " delete hdfs file [" + fileName + "] succeed\n");
                    } else {
                        logger.error("Delte File [" + fileName + "] failed");
                        System.out.println(df.format(new Date()) + " delete hdfs file [" + fileName + "] failed\n");
                    }
                } else {
                    logger.error("File[" + fileName + "] does not exists");
                }
            }
            fs.close();
        } catch (Exception e) {
            logger.error("When delete hdfs csv file connect hdfs faild " + e.getMessage());
        }
    }

}
