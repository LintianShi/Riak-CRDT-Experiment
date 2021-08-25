package experiment;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import client.SetClient;
import client.RiakExpClient;
import experiment.RiakEnvironment;
import generator.ExpGenerator;
import generator.SetExpGenerator;
import record.RiakOperation;

public class RiakExpRunner {
    private RiakEnvironment expEnvironment;

    private static int SERVER_NUM = 3;
    private static int THREAD_PER_SERVER = 1;
    private static int OP_PER_SEC = 300;

    private double intervalTime;
    private int clientNum;
    private Location testDataType;
    private ExpGenerator generator;
    private List<RiakClientThread> threadList = new LinkedList<>();


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
        generator = new SetExpGenerator(100);
        //线程初始化
        System.out.println("Init Client...");
        CountDownLatch countDownLatch = new CountDownLatch(clientNum);
            //线程绑定客户端
        for (int i = 0; i < clientNum; i++) {
            threadList.add(new RiakClientThread(new SetClient(expEnvironment.newClient()), generator, intervalTime, countDownLatch));
        }
        //启动线程
        System.out.println("Start Client...");
        for (RiakClientThread thread : threadList) {
            thread.run();
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

//        SetClient setClient = new SetClient(expEnvironment.newClient(), testDataType);
//        System.out.println(setClient.executeByArgs("contains", "Messi"));
//        System.out.println(setClient.executeByArgs("contains", "Lukaku"));
//        setClient.executeByArgs("add", "Messi");
//        setClient.executeByArgs("add", "Lukaku");
//        System.out.println(setClient.executeByArgs("contains", "Messi"));
//        System.out.println(setClient.executeByArgs("contains", "Lukaku"));
//        setClient.executeByArgs("remove", "Lukaku");
//        System.out.println(setClient.executeByArgs("contains", "Messi"));
//        System.out.println(setClient.executeByArgs("contains", "Lukaku"));
    }

    private void initTestDataType() {
        testDataType = new Location(new Namespace("dhset", "test"),"testdata");
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
    private CountDownLatch countDownLatch;

    public RiakClientThread(RiakExpClient client, ExpGenerator generator, double intervalTime, CountDownLatch countDownLatch) {
        this.riakClient = client;
        this.generator = generator;
        this.intervalTime = (long)(intervalTime * 1000);
        this.countDownLatch = countDownLatch;
    }

    public void run() {
        try {
            while (generator.isRunning()) {
                RiakOperation operation = generator.generate();
                Thread.sleep(intervalTime);
                System.out.println(riakClient.execute(operation));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            countDownLatch.countDown();
        }
    }
}
