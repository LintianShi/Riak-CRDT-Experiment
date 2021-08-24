package client;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.*;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakDatatype;
import com.basho.riak.client.core.query.crdt.types.RiakMap;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.*;
import java.util.concurrent.ExecutionException;

import client.RiakExpClient;

public class MapClient  {

    private final static int DEFAULTMAXCONNECTIONS = 50;
    private final static int DEFAULTMINCONNECTIONS = 10;
    private final static int RETRIES = 5;

    private int maxConnections = DEFAULTMAXCONNECTIONS;
    private int minConnections = DEFAULTMINCONNECTIONS;
    private int retires = DEFAULTMINCONNECTIONS;

    private RiakClient riakClient;
    private Location hymap;
    private Map localMap;
    /*
      @param hosts(hosts格式：127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083)
    */

    public MapClient(String hosts) throws Exception{

        String[] addresses = hosts.split(",");
        RiakNode.Builder riakNodeBuilder = new RiakNode.Builder()
                .withMinConnections(minConnections)
                .withMaxConnections(maxConnections);
        LinkedList<RiakNode> nodes = new LinkedList<RiakNode>();
        for (int i = 0; i < addresses.length; i++){
            int j = addresses[i].indexOf(":");
            nodes.add(riakNodeBuilder.withRemoteAddress(addresses[i].substring(0,j))
                    .withRemotePort(Integer.parseInt(addresses[i].substring(j+1))).build());
        }


        RiakCluster riakCluster = RiakCluster.builder(nodes).withExecutionAttempts(retires).build();
        riakCluster.start();
        riakClient = new RiakClient(riakCluster);

        hymap = new Location(new Namespace("dhmap","customers"), "hy_info");

        FetchMap fetchR = new FetchMap.Builder(hymap)
                .build();
        FetchMap.Response responseR = riakClient.execute(fetchR);
        RiakMap mapR = responseR.getDatatype();

        localMap = mapR.view();

    }

    public String mapPut(String key, String value) throws Exception{
//        String putResult = "";
//        if(localMap.containsKey(BinaryValue.create(key))){
//            putResult = localMap.get(BinaryValue.create(key)).toString();
//            putResult = putResult.substring(1,putResult.length()-1);
//        }
//        else{
//            putResult = "null";
//        }

        String putResult = mapGet(key);
        RegisterUpdate ru = new RegisterUpdate(value);
        MapUpdate muR = new MapUpdate()
                .update(key,ru);
        UpdateMap updateR = new UpdateMap.Builder(hymap, muR).build();

        riakClient.execute(updateR);

        return putResult;
    }

    public String mapGet(String key) throws Exception{
        String getResult = "";
        FetchMap fetchR = new FetchMap.Builder(hymap)
                .build();
        FetchMap.Response responseR = riakClient.execute(fetchR);
        RiakMap mapR = responseR.getDatatype();
        localMap = mapR.view();
        if(localMap.containsKey(BinaryValue.create(key))){
            getResult = localMap.get(BinaryValue.create(key)).toString();
            getResult = getResult.substring(1,getResult.length()-1);
        }
        else{
            getResult = "null";
        }
        return getResult;
    }

    public void mapClear() throws ExecutionException,InterruptedException{
        FetchMap fetch = new FetchMap.Builder(hymap).build();
        FetchMap.Response response = riakClient.execute(fetch);
        Context ctx = response.getContext();
        RiakMap map = response.getDatatype();
        Set<BinaryValue> keySet = new HashSet<>();
        keySet = map.view().keySet();
        for(BinaryValue key : keySet){
            MapUpdate removeUpdate = new MapUpdate()
                    .removeRegister(key.toString());
            UpdateMap update = new UpdateMap.Builder(hymap,removeUpdate)
                    .withContext(ctx)
                    .build();
            riakClient.execute(update);
//            System.out.println("Removing key "+ key.toString() +" from map!");
        }
        System.out.println("Finish clear");

    }

    public boolean mapContainsValue(String value) throws Exception{
        boolean result = false;
        FetchMap fetchR = new FetchMap.Builder(hymap)
                .build();
        FetchMap.Response responseR = riakClient.execute(fetchR);
        RiakMap mapR = responseR.getDatatype();
        localMap = mapR.view();

        Iterator<BinaryValue> it = localMap.keySet().iterator();
        while (it.hasNext()){
            BinaryValue currentK = it.next();
            String currentV = localMap.get(currentK).toString();
            currentV = currentV.substring(1,currentV.length()-1);
//            System.out.println("Search key:" + currentK.toString() + " Value:" + currentV);
            if(currentV.equals(value)){
//                System.out.println("Aha~");
                result = true;
                break;
            }
        }
        return result;
    }

    public void close() throws Exception{
        riakClient.shutdown();
    }

    public RiakClient getRiakClient() {
        return this.riakClient;
    }

    public static void main(String[] args) throws Exception{

        MapClient clusterRiakClient = new MapClient("127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
        System.out.println(clusterRiakClient.mapPut("first_name","Yi"));
        System.out.println(clusterRiakClient.mapPut("phone_number","12345678"));

        System.out.println("Contain Yi?:" + clusterRiakClient.mapContainsValue("Yi"));
        System.out.println(clusterRiakClient.mapPut("first_name","Xuemin2"));
        System.out.println(clusterRiakClient.mapGet("first_name"));
        Location hyMap = new Location(new Namespace("dhmap","customers"), "hy_info");
        RiakClient client = clusterRiakClient.getRiakClient();

//        测试Map内封装Register
        RegisterUpdate ru1 = new RegisterUpdate("Yi");
        RegisterUpdate ru2 = new RegisterUpdate("12345678");
        MapUpdate muR = new MapUpdate()
                .update("first_name",ru1)
                .update("phone_number", ru2);
        UpdateMap updateR = new UpdateMap.Builder(hyMap, muR)
                .build();
        client.execute(updateR);

        FetchMap fetchR = new FetchMap.Builder(hyMap)
                .build();
        FetchMap.Response responseR = client.execute(fetchR);
        RiakMap mapR = responseR.getDatatype();

        System.out.println("hy_info first_name:" + mapR.getRegister("first_name").view());
        System.out.println("do not exist:" + mapR.getRegister("last_name"));

        //ContainsValue的实现
        Map<BinaryValue, List<RiakDatatype>> m = mapR.view();
        Iterator<BinaryValue> it = m.keySet().iterator();
        while (it.hasNext()){
            BinaryValue currentK = it.next();
            String currentV = m.get(currentK).toString();
            currentV = currentV.substring(1,currentV.length()-1);
            System.out.println("Search key:" + currentK.toString() + " Value:" + currentV);
            if(currentV.equals("Yi")){
                System.out.println("Aha~");
                break;
            }
        }
        System.out.println(m.get(BinaryValue.create("phone_number")));
        System.out.println(mapR.view());


        Location testMap = new Location(new Namespace("dhmap","customers"), "xm_info");
        FetchMap fetchM = new FetchMap.Builder(testMap).build();
        FetchMap.Response responseM = client.execute(fetchM);
        RiakMap mapM = responseM.getDatatype();
        System.out.println(mapM.view());

        //测试Map内封装Flag
        MapUpdate muF = new MapUpdate()
                .update("enterprise_customer", new FlagUpdate(true));
        UpdateMap updateF = new UpdateMap.Builder(hyMap, muF)
                .build();
        client.execute(updateF);

        FetchMap fetchF = new FetchMap.Builder(hyMap).build();
        FetchMap.Response responseF = client.execute(fetchF);
        RiakMap mapF = responseF.getDatatype();
        System.out.println(mapF.getFlag("enterprise_customer").view());

        //测试Map内封装Counter
        CounterUpdate cu = new CounterUpdate(1);
        MapUpdate muC = new MapUpdate()
                .update("page_visits", cu);
        UpdateMap updateC = new UpdateMap.Builder(hyMap, muC).build();
        client.execute(updateC);



        clusterRiakClient.close();

    }

}
