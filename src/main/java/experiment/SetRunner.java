package experiment;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import experiment.RiakExpRunner;
import client.RiakExpClient;

public class SetRunner extends experiment.RiakExpRunner {
    protected void initTestDataType() {
        dataType = "Set";
        testDataType = new Location(new Namespace("dhset", "test"),"testdata");
    }

    protected RiakExpClient initClient(RiakClient riakClient, Location location) {
        return new client.SetClient(riakClient, location);
    }
}
