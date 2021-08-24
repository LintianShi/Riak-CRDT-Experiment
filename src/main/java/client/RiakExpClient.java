package client;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.query.Location;

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

    public abstract String execute(String operationName, String...args) throws Exception;
}
