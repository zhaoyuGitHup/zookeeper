package zookeeper;


import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class HelloZK {
    /**
     * Logger for this class
     */
    private static final Logger logger = Logger.getLogger(HelloZK.class);
    private static final String CONNECTSTRING = "49.232.29.45:2181";
    private static final String PATH = "/zhaoyu";
    private static final int SESSION_TIMEOUT = 50 * 1000;

    /**
     * 创建zookeeper
     * @return
     * @throws IOException
     */
    public ZooKeeper startZK() throws IOException {
        return new ZooKeeper(CONNECTSTRING, SESSION_TIMEOUT, new Watcher() {
              @Override
              public void process(WatchedEvent watchedEvent) {

              }
          });


    }

    /**
     * 关闭 zookeeper
     * @param zk  zookeeper
     * @throws InterruptedException
     */
    public void stopZK(ZooKeeper zk) throws InterruptedException {
        if (zk != null) {
            zk.close();
        }
    }

    /**
     * 创建znoode 节点数据
     * @param zk  zookeeper
     * @param path  节点路径
     * @param nodeValue  节点值
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void createZNode(ZooKeeper zk, String path, String nodeValue) throws KeeperException, InterruptedException {
        zk.create(path, nodeValue.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     *  获取znoode 节点的值
     * @param zk    zookeeper
     * @param path  路径
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String getZNode(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        byte[] byteArray = zk.getData(path, false, new Stat());
        return new String(byteArray);
    }

    /**
     * 测试
     * @param args
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        HelloZK hello = new HelloZK();
        ZooKeeper zk = hello.startZK();
        Stat stat = zk.exists(PATH, false);
        if (stat == null) {
            hello.createZNode(zk, PATH, "zk_0518");
            String result = hello.getZNode(zk, PATH);
            System.out.println("**********result: " + result);
        } else {
            System.out.println("***********znode has already ok***********");
        }
        hello.stopZK(zk);
    }
}