package org.conan.hadoop.hdfs;

import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;


public class HdfsDAO {

    private static final String HDFS = "hdfs://namenode:9000";

    public HdfsDAO(Configuration conf) {
        this(HDFS, conf);
    }

    public HdfsDAO(String hdfs, Configuration conf) {
        this.hdfsPath = hdfs;
        this.conf = conf;
    }

    private String hdfsPath;
    private Configuration conf;

    public static void main(String[] args) throws IOException {

        JobConf conf = config();
        HdfsDAO hdfs = new HdfsDAO(conf);
        hdfs.createFile("/input/kafka/test", "Hello world\n");
//        hdfs.copyFile("datafile/item.csv", "/tmp/new");
//        hdfs.ls("/input");
//        hdfs.rename("/user/hdfs/pagerank/tmp3", "/user/hdfs/pagerank/tmp4");
    }
    public void createFile(String file, String content) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        //InputStream in = new BufferedInputStream(new FileInputStream(file));
        try {
            //追加
            os = fs.append(new Path(file));
            os.write(buff, 0, buff.length);
            System.out.println("Success");
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
        JobConf conf = new JobConf(HdfsDAO.class);
        conf.setJobName("HdfsDAO");
        conf.addResource("D:\\Users\\zhanghang\\maven_hadoop_template-master\\src\\main\\resources\\hadoop\\core-site.xml");
        conf.addResource("D:\\Users\\zhanghang\\maven_hadoop_template-master\\src\\main\\resources\\hadoop\\hdfs-site.xml");
        conf.addResource("D:\\Users\\zhanghang\\maven_hadoop_template-master\\src\\main\\resources\\hadoop\\mapred-site.xml");
        return conf;
    }

    public void mkdirs(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }
        fs.close();
    }

    public void rmr(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.deleteOnExit(path);
        System.out.println("Delete: " + folder);
        fs.close();
    }

    public void rename(String src, String dst) throws IOException {
        Path name1 = new Path(src);
        Path name2 = new Path(dst);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.rename(name1, name2);
        System.out.println("Rename: from " + src + " to " + dst);
        fs.close();
    }

    public void ls(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FileStatus[] list = fs.listStatus(path);
        System.out.println("ls: " + folder);
        System.out.println("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDir(), f.getLen());
        }
        System.out.println("==========================================================");
        fs.close();
    }

    public void copyFile(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }

    public void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }

    public String cat(String remoteFile) throws IOException {
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FSDataInputStream fsdis = null;
        System.out.println("cat: " + remoteFile);

        OutputStream baos = new ByteArrayOutputStream();
        String str = null;

        try {
            fsdis = fs.open(path);
            IOUtils.copyBytes(fsdis, baos, 4096, false);
            str = baos.toString();
        } finally {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
        System.out.println(str);
        return str;
    }


    public void location() throws IOException {
        // String folder = hdfsPath + "create/";
        // String file = "t2.txt";
        // FileSystem fs = FileSystem.get(URI.create(hdfsPath), new
        // Configuration());
        // FileStatus f = fs.getFileStatus(new Path(folder + file));
        // BlockLocation[] list = fs.getFileBlockLocations(f, 0, f.getLen());
        //
        // System.out.println("File Location: " + folder + file);
        // for (BlockLocation bl : list) {
        // String[] hosts = bl.getHosts();
        // for (String host : hosts) {
        // System.out.println("host:" + host);
        // }
        // }
        // fs.close();
    }

}