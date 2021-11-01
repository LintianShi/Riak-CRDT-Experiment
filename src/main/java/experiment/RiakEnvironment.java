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
    public final static String availableIPs[] = {"172.24.81.136", "172.24.81.137", "172.24.81.132", "172.24.234.239", "172.24.234.240"};
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