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
import record.RiakOperation;

public class MapClient extends RiakExpClient {
    private Map localMap;

    public MapClient(RiakClient riakClient){
        super(riakClient);
    }

    public MapClient(RiakClient riakClient, Location location){
        super(riakClient, location);
    }

    private String put(String key, String value) throws ExecutionException, InterruptedException {
        String putResult = get(key);
        RegisterUpdate ru = new RegisterUpdate(value);
        MapUpdate muR = new MapUpdate()
                .update(key,ru);
        UpdateMap updateR = new UpdateMap.Builder(testDataType, muR).build();
        riakClient.execute(updateR);
        return putResult;
    }

    private String get(String key) throws ExecutionException, InterruptedException {
        String getResult = "";
        FetchMap fetchR = new FetchMap.Builder(testDataType)
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

    private boolean containsValue(String value) throws Exception{
        boolean result = false;
        FetchMap fetchR = new FetchMap.Builder(testDataType)
                .build();
        FetchMap.Response responseR = riakClient.execute(fetchR);
        RiakMap mapR = responseR.getDatatype();
        localMap = mapR.view();

        Iterator<BinaryValue> it = localMap.keySet().iterator();
        while (it.hasNext()){
            BinaryValue currentK = it.next();
            String currentV = localMap.get(currentK).toString();
            currentV = currentV.substring(1,currentV.length()-1);
            if(currentV.equals(value)){
                result = true;
                break;
            }
        }
        return result;
    }

    public String execute(RiakOperation operation) throws Exception {
        String operationName = operation.getOperationName();
        if (operationName.equals("get") || operationName.equals("containsValue")) {
            String key = operation.getArguments().get(0);
            return executeByArgs(operationName, key);
        } else if (operationName.equals("put")) {
            String key = operation.getArguments().get(0);
            String value = operation.getArguments().get(1);
            return executeByArgs(operationName, key, value);
        } else {
            throw new Exception("Invalid Op");
        }

    }

    public String executeByArgs(String operationName, String...args) throws Exception {
        try {
            if (operationName.equals("get")) {
                return get(args[0]);
            } else if (operationName.equals("put")) {
                return put(args[0], args[1]);
            } else if (operationName.equals("containsValue")) {
                boolean flag = containsValue(args[0]);
                if (flag) {
                    return "true";
                } else {
                    return "false";
                }
            } else {
                throw new Exception("Invalid Op");
            }
        } catch (Exception e) {
            throw new Exception(operationName + "_" + args[0]);
        }
    }
}
