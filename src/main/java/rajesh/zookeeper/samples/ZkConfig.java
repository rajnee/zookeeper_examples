package rajesh.zookeeper.samples;

/**
 * Created by rajesh on 1/31/16.
 */
public class ZkConfig {

    public static String getHostString() {
        return "127.0.0.1:2182,127.0.0.1:2183,127.0.0.1:2184";
    }

    public static int getTimeout() {
        return 2000;
    }
}
