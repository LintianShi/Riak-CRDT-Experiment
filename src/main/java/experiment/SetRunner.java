package experiment;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import experiment.RiakExpRunner;
import client.RiakExpClient;
import client.SetClient;
import generator.SetExpGenerator;
import generator.ExpGenerator;

public class SetRunner extends RiakExpRunner {
    protected void initTestDataType() {
        dataType = "Set";
        testDataType = new Location(new Namespace("testset", "test"),"testdata");
    }

    protected RiakExpClient initClient(RiakClient riakClient, Location location) {
        return new SetClient(riakClient, location);
    }

    protected ExpGenerator initGenerator(int totalOps, String pattern) {
        return new SetExpGenerator(totalOps, pattern);
    }
}
