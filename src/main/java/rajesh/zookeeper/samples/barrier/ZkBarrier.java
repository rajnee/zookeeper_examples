package rajesh.zookeeper.samples.barrier;

import org.apache.zookeeper.*;
import rajesh.zookeeper.samples.ZkBase;

import java.io.IOException;
import java.util.List;

/**
 * Created by nikita on 1/31/16.
 */
public class ZkBarrier extends ZkBase {

    public static final String BARRIER_ROOT = "/samples-barrier";
    private Object mutex = new Object();
    private String hostId;
    private static final int BARRIER_COUNT = 3;

    /* simulating a delay in processing the event
    * A delay in processing the event does not allow correct barrier recognition in this implementation
    */
    private boolean delay;

    public ZkBarrier(String hostId, String delay) {
        super();
        this.hostId = hostId;
        if (delay.equalsIgnoreCase("true")) {
            this.delay = true;
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("Got event:" + watchedEvent.getType().name());
        synchronized (mutex) {
            mutex.notify();
        }
    }

    public void runAfterBarrier(Runnable processor) throws Exception {
        ZooKeeper zk = getZookeeper();
        if (zk.exists(BARRIER_ROOT, true) == null) {
            zk.create(BARRIER_ROOT, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        enterBarrier();
        processor.run();
        leaveBarrier();
    }

    private void enterBarrier() throws Exception{
        ZooKeeper zk = getZookeeper();
        zk.create(BARRIER_ROOT + "/" + hostId, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        while(true) {
            synchronized (mutex) {
                if (delay) Thread.currentThread().sleep(2000L);
                List<String> children = zk.getChildren(BARRIER_ROOT, true);
                if (children.size() >= BARRIER_COUNT) {
                    return;
                }
                mutex.wait();
            }
        }
    }

    private void leaveBarrier() throws Exception {
        ZooKeeper zk = getZookeeper();
        zk.delete(BARRIER_ROOT + "/" + hostId, 0);
    }

    public static void main(String[] args) throws Exception{
        new ZkBarrier(args[0], args[1]).runAfterBarrier(new Runnable() {
            @Override
            public void run() {
                System.out.println("Barrier met, running at:" + System.currentTimeMillis());
            }
        });
    }
}
