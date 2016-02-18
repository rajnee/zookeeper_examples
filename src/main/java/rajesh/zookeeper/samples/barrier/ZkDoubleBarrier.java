package rajesh.zookeeper.samples.barrier;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import rajesh.zookeeper.samples.ZkBase;

import java.util.List;
import java.util.Random;

/**
 * Created by nikita on 1/31/16.
 */
public class ZkDoubleBarrier extends ZkBase {

    public static final String BARRIER_ROOT = "/double-barrier";
    private Object mutex = new Object();
    private String hostId;
    private String zkPath, fullZkPath;
    private static final int BARRIER_COUNT = 3;

    /* simulating a delay in processing the event
    * A delay in processing the event does not allow correct barrier recognition in this implementation
    */
    private boolean delay;

    public ZkDoubleBarrier(String hostId, String delay) {
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
        fullZkPath = zk.create(BARRIER_ROOT + "/" + hostId, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        zkPath = fullZkPath.substring(BARRIER_ROOT.length() + 1);
        System.out.println("Created node for host:" + fullZkPath + ", short =" + zkPath);
        while(true) {
            synchronized (mutex) {
                if (zk.exists(BARRIER_ROOT + "/ready", true) == null) {
                    List<String> children = zk.getChildren(BARRIER_ROOT, true);
                    if (children.size() >= BARRIER_COUNT) {
                        try {
                            zk.create(BARRIER_ROOT + "/ready", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                            return;
                        } catch (KeeperException e) {
                            if (e.code() == KeeperException.Code.NODEEXISTS)
                            {
                                return;
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } else {
                    return;
                }
                mutex.wait();
            }
        }
    }

    private void leaveBarrier() throws Exception {
        ZooKeeper zk = getZookeeper();
        while(true) {
            synchronized (mutex) {
                List<String> children = zk.getChildren(BARRIER_ROOT, false);
                print(children);
                if (children.size() == 0) {
                    return;
                }
                if (children.size() == 1 && children.contains("ready")) {
                    try {
                        zk.delete(BARRIER_ROOT + "/ready", 0);
                    } catch (InterruptedException e) {
                    } catch (KeeperException e) {
                    }
                    return;
                }
                //This is the only child, delete and return
                if (children.size() == 2 && zkPath != null && children.contains(zkPath) && children.contains("ready")) {
                    zk.delete(BARRIER_ROOT + "/ready", 0);
                    zk.delete(fullZkPath, 0); //Specify version as 0 to force a delete
                    return;
                }

                String minpath = null;
                String maxpath = null;
                int min = Integer.MAX_VALUE;
                int max = -1;
                for (String s: children) {
                    Integer i = getSequenceIdFromPath(s);
                    if (i != null) {
                        if (i > max) {
                            max = i;
                            maxpath = s;
                        }
                        if (i < min) {
                            min = i;
                            minpath = s;
                        }
                    }
                }

                if (zkPath != null && minpath.equals(zkPath)) {
                    //place a watch on highest node
                    if (!watchIfExists(BARRIER_ROOT, maxpath)) {
                        continue; //probably max node got deleted, let us get children again and run the logic
                    }

                } else {
                    //delete and place a watch on lowest node
                    if (zkPath != null) {
                        zk.delete(fullZkPath, 0);
                        zkPath = null;
                    }
                    if (!watchIfExists(BARRIER_ROOT, minpath)) {
                        continue; //Min node does not exist, check through loop again
                    }
                }
                mutex.wait();
            }
        }


    }

    private Integer getSequenceIdFromPath(String s) {
        if (s.length() > 10) {
            String k = s.substring(s.length() - 10);
            int i = Integer.parseInt(k);
            return i;
        }
        return null;
    }


    public static void main(String[] args) throws Exception{
        new ZkDoubleBarrier(args[0], args[1]).runAfterBarrier(new Runnable() {
            @Override
            public void run() {
                Random r = new Random();
                try {
                    Thread.currentThread().sleep((long)r.nextInt(2000));
                } catch (InterruptedException e) {
                }

                System.out.println("Barrier met, running at:" + System.currentTimeMillis());
            }
        });
    }
}
