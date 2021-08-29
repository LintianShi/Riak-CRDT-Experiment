package experiment;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import experiment.RiakExpRunner;
import client.RiakExpClient;
import client.MapClient;

public class MapRunner extends RiakExpRunner {
    protected void initTestDataType() {
        dataType = "Set";
        testDataType = new Location(new Namespace("testset", "test"),"testdata");
    }

    protected client.RiakExpClient initClient(RiakClient riakClient, Location location) {
        return new MapClient(riakClient, location);
    }
}
