package experiment;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.core.query.Location;

import java.util.LinkedList;
import java.util.List;

import client.SetClient;
import com.basho.riak.client.core.query.Namespace;
import experiment.RiakEnvironment;

public class RiakExpRunner {
    private RiakEnvironment expEnvironment;

    private static int SERVER_NUM = 3;
    private static int THREAD_PER_SERVER = 1;
    private static int OP_PER_SEC = 300;

    private double intervalTime;
    private Location testDataType;
    private List<RiakClientThread> threadList = new LinkedList<>();


    public RiakExpRunner() {
        expEnvironment = new RiakEnvironment(SERVER_NUM);
        intervalTime = (double)(SERVER_NUM * THREAD_PER_SERVER) / OP_PER_SEC;
    }

    public void run() throws Exception {
        //初始化环境，在构造函数里
        //初始化测试对象
        //线程初始化
            //线程绑定客户端
        //启动线程

        //结束线程
        //清楚测试对象
        //关闭环境
        initTestDataType();
        SetClient setClient = new SetClient(expEnvironment.newClient(), testDataType);
        System.out.println(setClient.execute("contains", "Messi"));
        System.out.println(setClient.execute("contains", "Lukaku"));
        setClient.execute("add", "Messi");
        setClient.execute("add", "Lukaku");
        System.out.println(setClient.execute("contains", "Messi"));
        System.out.println(setClient.execute("contains", "Lukaku"));
        setClient.execute("remove", "Lukaku");
        System.out.println(setClient.execute("contains", "Messi"));
        System.out.println(setClient.execute("contains", "Lukaku"));

        clean();
        shutdown();
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
    public void run() {}
}
