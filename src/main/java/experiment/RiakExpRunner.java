package experiment;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.io.File;

import client.SetClient;
import client.RiakExpClient;
import experiment.RiakEnvironment;
import generator.ExpGenerator;
import generator.SetExpGenerator;
import record.RiakOperation;
import record.RiakClientLog;

public class RiakExpRunner {
    private RiakEnvironment expEnvironment;

    private static int SERVER_NUM = 3;
    private static int THREAD_PER_SERVER = 1;
    private static int OP_PER_SEC = 300;
    private static int TOTAL_OPS = 500;

    private double intervalTime;
    private int clientNum;
    private String dataType = "Set";
    private String pattern = "Default";
    private Location testDataType;
    private ExpGenerator generator;
    private List<Thread> threadList = new LinkedList<>();
    private List<RiakClientLog> logs = new ArrayList<>();


    public RiakExpRunner() {
        expEnvironment = new RiakEnvironment(SERVER_NUM);
        intervalTime = (double)(SERVER_NUM * THREAD_PER_SERVER) / OP_PER_SEC;
        clientNum = SERVER_NUM * THREAD_PER_SERVER;
    }

    public void run() throws Exception {
        //初始化环境，在构造函数里
        System.out.println("Init Riak Environment...");
        //初始化测试对象
        initTestDataType();
        System.out.println("Init Test Data Type...");
        //初始化generator
        System.out.println("Init Wordload Generator...");
        generator = new SetExpGenerator(TOTAL_OPS);
        //初始化Log
        for (int i = 0; i < clientNum; i++) {
            logs.add(new RiakClientLog());
        }
        //线程初始化
        System.out.println("Init Client...");
        CountDownLatch countDownLatch = new CountDownLatch(clientNum);
        CyclicBarrier cyclicBarrier = new CyclicBarrier(clientNum);
            //线程绑定客户端
        for (int i = 0; i < clientNum; i++) {
            threadList.add(new Thread(new RiakClientThread(new SetClient(expEnvironment.newClient(), testDataType), generator, intervalTime, logs.get(i), countDownLatch, cyclicBarrier)));
        }
        //启动线程
        System.out.println("Start Client...");
        for (Thread thread : threadList) {
            thread.start();
        }
        //结束线程
        countDownLatch.await();
        System.out.println("Shutdown Client...");
        //清除测试对象
        System.out.println("Clean Test Data Type...");
        clean();
        //关闭环境
        System.out.println("Shutdown Environment...");
        shutdown();

        //输出日志
        outputTrace();
    }

    private void initTestDataType() {
        testDataType = new Location(new Namespace("dhset", "test"),"testdata");
    }

    private void outputTrace() {
        long ts = System.currentTimeMillis();
        String filename = "result/" + dataType + "_" + pattern + "_" + Integer.toString(SERVER_NUM) + "_" + Integer.toString(clientNum) + "_" + Integer.toString(OP_PER_SEC) + "_" + Long.toString(ts);
        File file = new File(filename);
        file.mkdir();
        try {
            for (int i = 0; i < logs.size(); i++) {
                logs.get(i).outputLog(filename + "/" + Integer.toString(i) + ".trc");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void clean() throws Exception {
        RiakClient riakClient = expEnvironment.newClient();
        DeleteValue delete = new DeleteValue.Builder(testDataType).build();
        riakClient.execute(delete);
    }

    private void shutdown() {
        expEnvironment.shutdown();
    }

    public static void main(String[] args) throws Exception {
        RiakExpRunner runner = new RiakExpRunner();
        runner.run();
    }
}

class RiakClientThread implements Runnable {
    private RiakExpClient riakClient;
    private ExpGenerator generator;
    private long intervalTime;
    private RiakClientLog threadLog;
    private CountDownLatch countDownLatch;
    private CyclicBarrier cyclicBarrier;

    public RiakClientThread(RiakExpClient client, ExpGenerator generator, double intervalTime, RiakClientLog threadLog, CountDownLatch countDownLatch, CyclicBarrier cyclicBarrier) {
        this.riakClient = client;
        this.generator = generator;
        this.intervalTime = (long)(intervalTime * 1000);
        this.threadLog = threadLog;
        this.countDownLatch = countDownLatch;
        this.cyclicBarrier = cyclicBarrier;
    }

    public void run() {
            while (generator.isRunning()) {
                try {
                    RiakOperation operation = generator.generate();
                    Thread.sleep(intervalTime);
                    String retValue = riakClient.execute(operation);
                    operation.setRetValue(retValue);
                    threadLog.appendLog(operation);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            countDownLatch.countDown();
    }
}
