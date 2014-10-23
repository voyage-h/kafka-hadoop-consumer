package org.conan.zk;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZkServer {
    private static ZooKeeper zk;
    private static String _conf_path;
    private Properties _props = new Properties();

    public ZkServer() throws IOException {
        _props.load(new FileInputStream(_conf_path));
        // 创建一个与服务器的连接
        try {
            zk = new ZooKeeper(_props.getProperty("zookeeper.connect"), 60000, new Watcher() {
                // 监控所有被触发的事件
                public void process(WatchedEvent event) {
                    System.out.println("EVENT:" + event.getType());
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //集群模式
        if (args != null && args.length > 0) {
            _conf_path = args[0];
        }
        else {
            _conf_path = "src/main/resources/hadoop/kafkaToHdfs.properties";
        }
        ZkServer zks = new ZkServer();
        zks.ls();
        // 关闭连接
        zk.close();
    }

    // 查看节点
    public void ls() throws KeeperException, InterruptedException {
        //根节点
        System.out.println("ls / => " + zk.getChildren("/", true));
        //brokers节点
        System.out.println("ls / => " + zk.getChildren("/brokers/ids/0", true));
    }

    // 创建一个目录节点
    public void creatDirNode() throws KeeperException, InterruptedException {
        if (zk.exists("/node", true) == null) {
            zk.create("/node", "conan".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("create /node conan");
            // 查看/node节点数据
            System.out.println("get /node => " + new String(zk.getData("/node", false, null)));
            // 查看根节点
            System.out.println("ls / => " + zk.getChildren("/", true));
        }
    }

    // 创建一个子目录节点
    public void create() throws KeeperException, InterruptedException {
        if (zk.exists("/node/sub1", true) == null) {
            zk.create("/node/sub1", "sub1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("create /node/sub1 sub1");
            // 查看node节点
            System.out.println("ls /node => " + zk.getChildren("/node", true));
        }
    }

    // 修改节点数据
    public void modify() throws KeeperException, InterruptedException {
        if (zk.exists("/node", true) != null) {
            zk.setData("/node", "changed".getBytes(), -1);
            // 查看/node节点数据
            System.out.println("get /node => " + new String(zk.getData("/node", false, null)));
        }
    }

    // 删除节点
    public void delete() throws KeeperException, InterruptedException {
        if (zk.exists("/node/sub1", true) != null) {
            zk.delete("/node/sub1", -1);
            zk.delete("/node", -1);
            // 查看根节点
            System.out.println("ls / => " + zk.getChildren("/", true));
        }
    }

}
