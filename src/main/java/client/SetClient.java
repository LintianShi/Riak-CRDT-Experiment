package client;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.Context;
import com.basho.riak.client.api.commands.datatypes.FetchSet;
import com.basho.riak.client.api.commands.datatypes.SetUpdate;
import com.basho.riak.client.api.commands.datatypes.UpdateSet;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakSet;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import client.RiakExpClient;
import record.RiakOperation;

public class SetClient extends RiakExpClient {
    public SetClient(RiakClient riakClient){
        super(riakClient);
    }

    public SetClient(RiakClient riakClient, Location location){
        super(riakClient, location);
    }

    private void add(String element) throws ExecutionException, InterruptedException {
        SetUpdate su = new SetUpdate()
                .add(element);
        UpdateSet update = new UpdateSet.Builder(testDataType, su)
                .build();
        this.riakClient.execute(update);
    }

    private void remove(String element) throws ExecutionException, InterruptedException {
        FetchSet fetch = new FetchSet.Builder(testDataType).build();
        FetchSet.Response response = this.riakClient.execute(fetch);
        Context ctx = response.getContext(); //Causal context
        SetUpdate su = new SetUpdate()
                .remove(element);
        UpdateSet update = new UpdateSet.Builder(testDataType, su)
                .withContext(ctx)
                .build();
        this.riakClient.execute(update);
    }

    private boolean contains(String element) throws ExecutionException, InterruptedException {
        FetchSet fetch = new FetchSet.Builder(testDataType).build();
        FetchSet.Response response = this.riakClient.execute(fetch);
        RiakSet set = response.getDatatype();
        return set.contains(element);
    }

    private int size() throws ExecutionException, InterruptedException {
        FetchSet fetch = new FetchSet.Builder(testDataType).build();
        FetchSet.Response response = this.riakClient.execute(fetch);
        RiakSet set = response.getDatatype();
        return set.view().size();
    }

    public String execute(RiakOperation operation) throws Exception {
        String operationName = operation.getOperationName();
        String element = "dummy";
        if (!operationName.equals("size")) {
            element = operation.getArguments().get(0);
        }
        return executeByArgs(operationName, element);
    }

    public String executeByArgs(String operationName, String...args) throws Exception {
        try {
            if (operationName.equals("add")) {
                add(args[0]);
                return "null";
            } else if (operationName.equals("remove")) {
                remove(args[0]);
                return "null";
            } else if (operationName.equals("contains")) {
                boolean flag = contains(args[0]);
                if (flag) {
                    return "true";
                } else {
                    return "false";
                }
            } else if (operationName.equals("size")) {
                int sz = size();
                return Integer.toString(sz);
            } else {
                throw new Exception("Invalid Op");
            }
        } catch (Exception e) {
            throw new Exception(operationName);
        }
        
    }
}
