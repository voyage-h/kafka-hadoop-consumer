package org.conan.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;


public class HdfsDAO1 {

    private static final String HDFS = "hdfs://namenode:9000";
    public HdfsDAO1(Configuration conf) {
        this(HDFS, conf);
    }
    public HdfsDAO1(String hdfs, Configuration conf) {
        this.hdfsPath = hdfs;
        this.conf = conf;
    }
    private String hdfsPath;
    private Configuration conf;

    public static void main(String[] args) throws IOException {

        JobConf conf = config();
        HdfsDAO1 hdfs = new HdfsDAO1(conf);
        hdfs.createFile("/input/kafka/text", "Hello world!!");
    }
    public void createFile(String file, String content) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(file));
            os.write(buff, 0, buff.length);
            System.out.println("Create: " + file);
        } finally {
            if (os != null)
                os.close();
        }
        fs.close();
    }
    public void w(String folder) throws IOException {
        String localSrc = "D:\\Users\\zhanghang\\quleStormDev\\branches\\storm_zhanghang\\Storm_qule\\src\\main\\resources\\adrealtime.txt";
        String dst = "hdfs://namenode:9000/input/kafka";

        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);//创建文件系统实例
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            public void progress() {
                System.out.print(".");
            }//通过实例创建管道 create方法返回的是FSDataOutputStream对象
        });

        IOUtils.copyBytes(in, out, 4096, true);//通过管道进行写入
    }
    public static JobConf config() {
        JobConf conf = new JobConf(HdfsDAO1.class);
        conf.setJobName("HdfsDAO");
        conf.addResource("D:\\Users\\zhanghang\\maven_hadoop_template-master\\src\\main\\resources\\hadoop\\core-site.xml");
        conf.addResource("D:\\Users\\zhanghang\\maven_hadoop_template-master\\src\\main\\resources\\hadoop\\hdfs-site.xml");
        conf.addResource("D:\\Users\\zhanghang\\maven_hadoop_template-master\\src\\main\\resources\\hadoop\\mapred-site.xml");
        return conf;
    }
}