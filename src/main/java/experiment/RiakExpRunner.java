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
import experiment.SetRunner;
import experiment.MapRunner;
import generator.ExpGenerator;
import generator.SetExpGenerator;
import record.RiakOperation;
import record.RiakClientLog;

public abstract class RiakExpRunner {
    private RiakEnvironment expEnvironment;

    private static int SERVER_NUM = 3;
    private static int THREAD_PER_SERVER = 1;
    private static int TOTAL_OPS = 100;
    private static String WORKLOAD_PATTERN = "default";
    private static int CLIENT_NUM = 3;

    protected String dataType;
    protected Location testDataType;
    private ExpGenerator generator;
    private List<Thread> threadList = new LinkedList<>();
    private List<RiakClientLog> logs = new ArrayList<>();


    public RiakExpRunner() {
        expEnvironment = new RiakEnvironment(SERVER_NUM);
    }

    public void run() throws Exception {
        //初始化环境，在构造函数里
        System.out.println("Init Riak Environment...");
        //初始化测试对象
        initTestDataType();
        System.out.println("Init Test Data Type...");
        //初始化generator
        System.out.println("Init Wordload Generator...");
        generator = initGenerator(TOTAL_OPS, WORKLOAD_PATTERN);
        //初始化Log
        for (int i = 0; i < CLIENT_NUM; i++) {
            logs.add(new RiakClientLog());
        }
        //线程初始化
        System.out.println("Init Client...");
        CountDownLatch countDownLatch = new CountDownLatch(CLIENT_NUM);
        //线程绑定客户端
        for (int i = 0; i < CLIENT_NUM; i++) {
            threadList.add(new Thread(new RiakClientThread(initClient(expEnvironment.newClient(), testDataType), generator, logs.get(i), countDownLatch)));
        }
        //启动线程
        Long startTime = System.currentTimeMillis();
        System.out.println("Start Client...");
        for (Thread thread : threadList) {
            thread.start();
        }
        //结束线程
        countDownLatch.await();
        Long endTime = System.currentTimeMillis();
        System.out.println("Shutdown Client...");
        //清除测试对象
        System.out.println("Clean Test Data Type...");
        clean();
        //关闭环境
        System.out.println("Shutdown Environment...");
        shutdown();
        System.out.printf("Total %d ms, %d ops \n", endTime - startTime, generator.getTotalOps());

        //输出日志
        outputTrace();
    }

    protected abstract void initTestDataType();

    protected abstract RiakExpClient initClient(RiakClient riakClient, Location location);

    protected abstract ExpGenerator initGenerator(int totalOps, String pattern);

    private void outputTrace() {
        long ts = System.currentTimeMillis();
        String filename = "result/" + dataType + "_" + WORKLOAD_PATTERN + "_" + Integer.toString(SERVER_NUM) + "_" + Integer.toString(CLIENT_NUM) + "_" + Integer.toString(TOTAL_OPS) + "_" + Long.toString(ts);
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
        RiakExpRunner runner = new SetRunner();
        runner.run();
    }
}

class RiakClientThread implements Runnable {
    private RiakExpClient riakClient;
    private ExpGenerator generator;
    private RiakClientLog threadLog;
    private CountDownLatch countDownLatch;

    public RiakClientThread(RiakExpClient client, ExpGenerator generator, RiakClientLog threadLog, CountDownLatch countDownLatch) {
        this.riakClient = client;
        this.generator = generator;
        this.threadLog = threadLog;
        this.countDownLatch = countDownLatch;
    }

    public void run() {
        while (generator.isRunning()) {
            try {
                RiakOperation operation = generator.get();
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
