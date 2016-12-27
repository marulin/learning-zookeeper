package com.xianglesong.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

public class ZookeeperCRUD {

  public static void main(String[] args) throws Exception {
    String zookeeperConnectionString = "localhost:2181";
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework client =
        CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
    client.start();

    //
    // client.create().forPath("/my", "my".getBytes());

    // test
    // try {
    // client.create().forPath("/my/path", "myData".getBytes());
    // } catch (Exception e) {
    // System.err.println(e.getMessage());
    // e.printStackTrace();
    // }

    // client.delete().forPath("/my/path");
    Stat stat = client.checkExists().forPath("/my/path");
    System.out.println(stat);

    client.close();
  }
}
