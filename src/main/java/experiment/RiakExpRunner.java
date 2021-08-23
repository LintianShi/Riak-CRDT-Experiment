package experiment;

import experiment.RiakEnvironment;

public class RiakExpRunner {
    private RiakEnvironment expEnvironment;

    private static int SERVER_NUM = 3;
    private static int THREAD_PER_SERVER = 1;
    private static int OP_PER_SEC = 300;
    private static double INTERVAL_TIME;

    public RiakExpRunner() {
        expEnvironment = new RiakEnvironment(SERVER_NUM);
        INTERVAL_TIME = (double)(SERVER_NUM * THREAD_PER_SERVER) / OP_PER_SEC;
    }

    public void run() {}

}
