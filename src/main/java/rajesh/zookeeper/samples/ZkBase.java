package rajesh.zookeeper.samples;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * Created by rajesh on 1/31/16.
 */
public abstract class ZkBase implements Watcher {

    protected ZooKeeper zk;

    protected String hostString;
    protected int timeout;
    protected Watcher watcher;

    @Override
    public void process(WatchedEvent watchedEvent) {

    }

    public ZkBase() {
        this(ZkConfig.getHostString(), ZkConfig.getTimeout());
    }

    public ZkBase(String hostString, int timeout) {
        this(hostString, timeout, null);
    }

    public ZkBase(String hostString, int timeout, Watcher watcher) {
        if (watcher == null) {
            watcher = this;
        }
        this.hostString = hostString;
        this.timeout = timeout;
        this.watcher = watcher;
    }

    public ZooKeeper getZookeeper() throws IOException {
        if (zk == null) {
            zk = new ZooKeeper(hostString, timeout, watcher);
        }
        return zk;
    }

    public boolean watchIfExists(String root, String path) throws KeeperException, InterruptedException {
        return watchIfExists(root + "/" + path);
    }

    public boolean watchIfExists(String path) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, true);
        return stat != null;
    }

    public boolean watchIfNotExists(String path) throws KeeperException, InterruptedException {
        return !watchIfExists(path);
    }

    public void watch(String path) throws KeeperException, InterruptedException {
        zk.exists(path, true);
    }

    public final void print(List<String> stringList) {
        StringBuilder sb  = new StringBuilder();
        for (String s:stringList) {
            sb.append(s).append(",");
        }
        System.out.println(sb.toString());
    }
}
