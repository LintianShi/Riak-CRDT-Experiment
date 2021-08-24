package experiment;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class RiakEnvironment {
    private final static int DEFAULT_MAX_CONNECTIONS = 500;
    private final static int DEFAULT_MIN_CONNECTIONS = 10;
    private final static int DEFAULT_RETRIES = 1;
    private final static String availableIPs[] = {"172.21.252.91", "172.21.252.92", "172.21.252.93", "172.22.119.10", "172.22.119.11", "172.22.119.12"};
    private final static int port = 8087;

    private RiakCluster riakCluster;
    private List<String> ips = new ArrayList<>();


    public RiakEnvironment(int serverNum) {
        for (int i = 0; i < serverNum; i++) {
            ips.add(availableIPs[i]);
        }

        RiakNode.Builder riakNodeBuilder = new RiakNode.Builder()
                .withMinConnections(DEFAULT_MIN_CONNECTIONS)
                .withMaxConnections(DEFAULT_MAX_CONNECTIONS);

       List<RiakNode> nodes = new ArrayList<>();
       for (String ip : ips) {
           nodes.add(riakNodeBuilder.withRemoteAddress(ip)
                   .withRemotePort(port).build());
       }
        this.riakCluster = RiakCluster.builder(nodes).withExecutionAttempts(DEFAULT_RETRIES).build();
        this.riakCluster.start();
    }

    public RiakClient newClient() {
        return new RiakClient(this.riakCluster);
    }

    public Future<Boolean> shutdown() {
        return this.riakCluster.shutdown();
    }


}