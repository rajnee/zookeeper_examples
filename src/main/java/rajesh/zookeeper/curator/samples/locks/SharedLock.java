package rajesh.zookeeper.curator.samples.locks;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import rajesh.zookeeper.samples.ZkConfig;

/**
 * Created by nikita on 2/5/16.
 */
public class SharedLock {

    private static final String LOCK_PATH = "curatorlock";

    private CuratorFramework curatorFramework;
    private InterProcessMutex mutex;

    public SharedLock(String host) {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder.connectionTimeoutMs(5000).connectString(host);
        builder.retryPolicy(new ExponentialBackoffRetry(500, 3));
        builder.namespace(LOCK_PATH);
        curatorFramework = builder.build();
        mutex = new InterProcessMutex(curatorFramework, "/lock1");
        curatorFramework.start();
    }

    public void lockAndRun(Runnable runnable) throws Exception {
        try {
            mutex.acquire();
            runnable.run();
            Stat stat = curatorFramework.checkExists().forPath("/number");
            if (stat == null) {
                System.out.println("Bytes:" + curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath("/number", "1".getBytes()).toCharArray());
            } else {
                byte[] b = curatorFramework.getData().forPath("/number");
                String s = new String(b);
                System.out.println("Data is [" + s + "]");
                Integer i = Integer.parseInt(s);
                int j = i  + 1;
                curatorFramework.setData().forPath("/number", ("" + j).getBytes());
            }
        } finally {
            if (mutex.isAcquiredInThisProcess()) {
                mutex.release();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        SharedLock sharedLock = new SharedLock(ZkConfig.getHostString());
        sharedLock.lockAndRun(new Runnable() {
            @Override
            public void run() {
                System.out.println("Lock acquired and running");
                try {
                    Thread.currentThread().sleep(2000L);
                } catch (InterruptedException e) {

                }
            }
        });
    }
}
