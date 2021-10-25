package experiment;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import experiment.RiakExpRunner;
import client.RiakExpClient;
import client.MapClient;
import generator.MapExpGenerator;
import generator.ExpGenerator;

public class MapRunner extends RiakExpRunner {
    public MapRunner(int n) {
        super(n);
    }

    protected client.RiakExpClient initClient(RiakClient riakClient, Location location) {
        return new MapClient(riakClient, location);
    }

    protected ExpGenerator initGenerator(int totalOps, String pattern) {
        return new MapExpGenerator(totalOps, pattern);
    }
}
