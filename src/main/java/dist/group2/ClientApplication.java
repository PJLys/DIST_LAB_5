package dist.group2;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.io.IOException;
import java.net.InetAddress;

@SpringBootApplication
public class ClientApplication {
    public static ApplicationContext context;
    private DiscoveryClient discoveryClient;
    Thread replicationthread;

    @Autowired
    public ClientApplication(DiscoveryClient discoveryClient) throws IOException {
        this.discoveryClient = discoveryClient;
        String name = InetAddress.getLocalHost().getHostName();
        String IPAddress = InetAddress.getLocalHost().getHostAddress();

        String multicastIP = "224.0.0.5";       // A random IP in the 224.0.0.0 to 239.255.255.255 range (reserved for multicast)
        InetAddress multicastGroup = InetAddress.getByName(multicastIP);
        int multicastPort = 4446;
        int unicastPortDiscovery = 4449;
        int namingPort = 8080;
        int fileUnicastPort = 4451;

        Communicator.init(multicastGroup, multicastPort, fileUnicastPort, multicastIP, unicastPortDiscovery);
        this.discoveryClient.init(name, IPAddress, unicastPortDiscovery, namingPort);
        ReplicationClient replicationClient = new ReplicationClient(fileUnicastPort);

        System.out.println("<---> " + name + " Instantiated with IP " + IPAddress + " <--->");
        discoveryClient.bootstrap();
        NamingClient.setBaseUrl(discoveryClient.getBaseUrl());
        NamingClient.setName(name);

        replicationClient.addFiles();
        replicationClient.setFileDirectoryWatchDog();
        replicationClient.replicateFiles();

        replicationthread = new Thread(replicationClient);
        replicationthread.start();
    }

    @PreDestroy
    public void shutdown() {
        replicationthread.stop();
    }

    public static void main(String[] args) {
        // Run Client
        context = SpringApplication.run(ClientApplication.class, args);
    }
}