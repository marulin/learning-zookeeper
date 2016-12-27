package com.xianglesong.zookeeper.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.util.concurrent.CountDownLatch;

/**
 * ZookeeperMonitor
 *
 * @author ma.rulin
 * @date 16/11/17
 */
public class ZookeeperMonitor implements Runnable, Watcher {
  private static String configPath = "/config";

  private static CuratorFramework client = null;

  private static PathChildrenCache cache = null;

  private KeeperState state;

  private CountDownLatch clientConnected;

  private boolean connected;

  public static void init() {
    System.out.println("init");
    try {
      String connStr = "localhost:2181";
      configPath = "/config";
      System.out.println("zookeeper conn str: " + connStr);
      client = CuratorFrameworkFactory.newClient(connStr, new ExponentialBackoffRetry(1000, 3));
      client.start();

      // in this example we will cache data. Notice that this is optional.
      cache = new PathChildrenCache(client, configPath, true);
      cache.start();

      PathChildrenCacheListener listener = new PathChildrenCacheListener() {
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
            throws Exception {
          switch (event.getType()) {
            case CHILD_ADDED: {
              System.out.println("Node added: "
                  + ZKPaths.getNodeFromPath(event.getData().getPath()));
              break;
            }

            case CHILD_UPDATED: {
              System.out.println("Node changed: "
                  + ZKPaths.getNodeFromPath(event.getData().getPath()));
              break;
            }

            case CHILD_REMOVED: {
              System.out.println("Node removed: "
                  + ZKPaths.getNodeFromPath(event.getData().getPath()));
              break;
            }
          }
        }
      };

      cache.getListenable().addListener(listener);

      System.out.println("monitor add");
    } catch (Exception e) {
      System.out.println(e.toString());
      e.printStackTrace();
    } finally {
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          CloseableUtils.closeQuietly(cache);
          CloseableUtils.closeQuietly(client);
        }
      });
    }
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    // watch
    System.out.println("Watcher " + this + " got event " + watchedEvent + " path:"
        + watchedEvent.getPath() + " type:" + watchedEvent.getType());

    state = watchedEvent.getState();
    if (state == Event.KeeperState.SyncConnected) {
      connected = true;
      clientConnected.countDown();
      //
    } else if (state == Event.KeeperState.Expired) {
      connected = false;
      System.out
          .println("Our previous ZooKeeper session was expired. Attempting to reconnect to recover relationship with ZooKeeper...");
      // retry
    }

  }

  @Override
  public void run() {
    // 定时监控系统是否正常
  }

  public static void main(String[] args) {
    // ZookeeperMonitor zkm = new ZookeeperMonitor();
    ZookeeperMonitor.init();
    // for (;;);
    while (true) {
      System.out.println("xx");
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
