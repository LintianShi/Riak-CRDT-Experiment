package client;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.query.Location;

import record.RiakOperation;

public abstract class RiakExpClient {
    protected RiakClient riakClient;
    protected Location testDataType;

    public RiakExpClient(RiakClient riakClient){
        this.riakClient = riakClient;
    }

    public RiakExpClient(RiakClient riakClient, Location location){
        this.riakClient = riakClient;
        this.testDataType = location;
    }

    public abstract String execute(RiakOperation operation) throws Exception;

    public abstract String executeByArgs(String operationName, String...args) throws Exception;
}
