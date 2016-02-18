package rajesh.zookeeper.samples.sample1;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import rajesh.zookeeper.samples.ZkConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by nikita on 1/31/16.
 */
public class ZkConnect implements Watcher {

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("Got event:" + watchedEvent.getType().name());
    }

    public void initAndRun() throws Exception {
        ZooKeeper zk = new ZooKeeper(ZkConfig.getHostString(), ZkConfig.getTimeout(), this);
        try {
            for (int i=0; i < 1000; i++) {
                String s = zk.create("/HelloWorld1", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                System.out.println("Created node:" + s);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<String> children = zk.getChildren("/", false);
        for (String s: children) {
//            System.out.println(s);
        }
        Object o = new Object();
        synchronized (o) {
            o.wait(50000L);
        }
    }
    public static void main(String[] args) throws Exception{
        new ZkConnect().initAndRun();
    }
}
