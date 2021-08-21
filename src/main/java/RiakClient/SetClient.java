package RiakClient;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.Context;
import com.basho.riak.client.api.commands.datatypes.FetchSet;
import com.basho.riak.client.api.commands.datatypes.SetUpdate;
import com.basho.riak.client.api.commands.datatypes.UpdateSet;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakSet;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class SetClient implements AutoCloseable {

    private final static int DEFAULTMAXCONNECTIONS = 500;
    private final static int DEFAULTMINCONNECTIONS = 10;
    private final static int RETRIES = 5;

    private int maxConnections = DEFAULTMAXCONNECTIONS;
    private int minConnections = DEFAULTMINCONNECTIONS;
    private int retires = DEFAULTMINCONNECTIONS;

    private RiakClient riakClient;
    private Location hyset;

//    private ObjectMapper myMapper;


    /*
      @param hosts(hosts格式：127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083)
    */

    public SetClient(String hosts) throws Exception{

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
//        myMapper = new ObjectMapper();
        hyset = new Location(new Namespace("dhset", "TestData"),"data");

    }

    public boolean setAdd(String value) throws ExecutionException, InterruptedException{
        boolean addResult = false;
        if(!this.setContains(value)){
            addResult = true;
        }
        else{
            addResult = false;
        }
        SetUpdate su = new SetUpdate()
                .add(value);
        UpdateSet update = new UpdateSet.Builder(hyset, su)
                .build();
        this.riakClient.execute(update);
        return addResult;
    }

    public boolean setRemove(String value) throws ExecutionException, InterruptedException{
        boolean removeResult = false;
        if(this.setContains(value)){
            removeResult = true;
        }
        else{
            removeResult = false;
        }
        FetchSet fetch = new FetchSet.Builder(this.hyset).build();
        FetchSet.Response response = this.riakClient.execute(fetch);
        Context ctx = response.getContext(); //Causal context
        SetUpdate su = new SetUpdate()
                .remove(value);
        UpdateSet update = new UpdateSet.Builder(hyset, su)
                .withContext(ctx)
                .build();
        this.riakClient.execute(update);
        return removeResult;
    }

    public boolean setContains(String value) throws ExecutionException, InterruptedException {
        boolean containResult = false;
        FetchSet fetch = new FetchSet.Builder(this.hyset).build();
        FetchSet.Response response = this.riakClient.execute(fetch);
        RiakSet set = response.getDatatype();
        if(set.contains(value)){
            containResult = true;
        }
        else{
            containResult = false;
        }
        return containResult;
    }

    public int setSize() throws ExecutionException, InterruptedException {
        FetchSet fetch = new FetchSet.Builder(this.hyset).build();
        FetchSet.Response response = this.riakClient.execute(fetch);
        RiakSet set = response.getDatatype();
        Set<BinaryValue> localSet = set.view();
        return localSet.size();
    }

    public boolean isEmpty() throws ExecutionException, InterruptedException {
        FetchSet fetch = new FetchSet.Builder(this.hyset).build();
        FetchSet.Response response = this.riakClient.execute(fetch);
        RiakSet set = response.getDatatype();
        Set<BinaryValue> localSet = set.view();
        return localSet.isEmpty();
    }

    public void setClear() throws ExecutionException, InterruptedException{
        FetchSet fetch = new FetchSet.Builder(this.hyset).build();
        FetchSet.Response response = this.riakClient.execute(fetch);
        RiakSet set = response.getDatatype();
        Set<BinaryValue> localSet = set.view();
        for(BinaryValue value: localSet){
            this.setRemove(value.toString());
        }
//        System.out.println("Finish Set clear.");
    }


    public void close() throws Exception{
        this.riakClient.shutdown();
    }

    public RiakClient getRiakClient() {
        return this.riakClient;
    }

    public static void main(String[] args) throws Exception{
        SetClient clusterRiakClient = new SetClient("172.21.252.92:8087,172.21.252.91:8087,172.21.252.93:8087");
        Location citiesSet = new Location(new Namespace("dhset", "travel"),"cities");
        FetchSet fetch = new FetchSet.Builder(citiesSet).build();
        FetchSet.Response response = clusterRiakClient.getRiakClient().execute(fetch);
        RiakSet set = response.getDatatype();
        boolean isEmpty = set.view().isEmpty();
        if(isEmpty){
            System.out.println("The set is empty...");
        }

        SetUpdate su = new SetUpdate()
                .add("Edinburgh")
                .add("London");
        UpdateSet update =  new UpdateSet.Builder(citiesSet, su)
                .build();
        clusterRiakClient.getRiakClient().execute(update);

        FetchSet.Response response1 = clusterRiakClient.getRiakClient().execute(fetch);
        isEmpty = response1.getDatatype().view().isEmpty();
        if(isEmpty){
            System.out.println("The set is empty...");
        }
        else{
            System.out.println("The set is not empty now");
        }

        Set<BinaryValue> binarySet = response1.getDatatype().view();

        for(BinaryValue city: binarySet){
            System.out.println(city.toStringUtf8());
            if(city.toStringUtf8().equals("London")){
                System.out.println("Get London!");
            }
        }

        String s = "London";
        RiakSet riakSet = response1.getDatatype();
        if(riakSet.contains("London")){
            System.out.println("Correct way to get London!");
        }
        if(riakSet.contains("Edinburgh")){
            System.out.println("Correct way to get Ed!");
        }


        System.out.println("Another way to test contain:");
        System.out.println(binarySet.contains(BinaryValue.create("London")));
        System.out.println(binarySet.size());

        System.out.println("Test remove.");

        FetchSet fetchBFRemove = new FetchSet.Builder(citiesSet).build();
        FetchSet.Response responseBFRemore = clusterRiakClient.getRiakClient().execute(fetchBFRemove);

        Context ctx = responseBFRemore.getContext();
        SetUpdate suRemove = new SetUpdate()
                .remove("London")
//                .remove("CCC")
                .remove("Edinburgh");
        UpdateSet updateRM = new UpdateSet.Builder(citiesSet, suRemove)
                .withContext(ctx)
                .build();
        clusterRiakClient.getRiakClient().execute(updateRM);

        FetchSet.Response response2 = clusterRiakClient.getRiakClient().execute(fetch);
        isEmpty = response2.getDatatype().view().isEmpty();
        if(isEmpty){
            System.out.println("The set is empty...");
        }
        else{
            System.out.println("The set is not empty now");
        }

        riakSet = response2.getDatatype();
        if(riakSet.contains("Edinburgh")){
            System.out.println("Wrong way to get Ed!");
        }
        else{
            System.out.println("Ed is not here.");
        }

        Set<BinaryValue> binarySet1 = response2.getDatatype().view();

        for(BinaryValue city: binarySet1){
            System.out.println(city.toStringUtf8());
        }

        clusterRiakClient.close();


    }


}
