package org.conan.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.security.PrivateKey;
import java.sql.SQLException;
import java.util.Queue;


public class HdfsUtil {
    private String hdfsURL;
    private Configuration conf;

    public HdfsUtil(String hdfs, Configuration config){
        this.hdfsURL = hdfs;
        this.conf = config;
    }
    public void write(String file, String content) throws IOException {
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        FileSystem fs = FileSystem.get(URI.create(hdfsURL), conf);
        try {
            //追加
            os = fs.append(new Path(file));
        } catch (FileNotFoundException e) {
            os = fs.create(new Path(file));
        } finally {
            os.write(buff, 0, buff.length);
            if (os != null)
                os.close();
        }
        fs.close();
    }
    public void batchWrite(String file,Queue<String>messages) throws IOException {
        String mes;
        FSDataOutputStream os = null;
        FileSystem fs = FileSystem.get(URI.create(hdfsURL), conf);
        try {
            //追加
            os = fs.append(new Path(file));
        } catch (FileNotFoundException e) {
            os = fs.create(new Path(file));
        } finally {
            while ((mes = messages.poll()) != null) {
                byte[] buff = mes.getBytes();
                os.write(buff, 0, buff.length);
            }
            if (os != null)
                os.close();
        }
        fs.close();
    }
}