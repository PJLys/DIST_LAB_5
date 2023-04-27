package dist.group2;

import dist.group2.Discovery.DiscoveryClient;
import dist.group2.Naming.NamingClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.ip.udp.UnicastReceivingChannelAdapter;

import java.io.IOException;
import java.net.InetAddress;

@SpringBootApplication
public class ClientApplication {
    public static ApplicationContext context;

    public ClientApplication() throws IOException {
        String name = InetAddress.getLocalHost().getHostName();
        String IPAddress = InetAddress.getLocalHost().getHostAddress();

        String multicastIP = "224.0.0.5";       // A random IP in the 224.0.0.0 to 239.255.255.255 range (reserved for multicast)
        InetAddress multicastGroup = InetAddress.getByName(multicastIP);
        int multicastPort = 4446;
        int unicastPortDiscovery = 4449;
        int namingPort = 8080;

        Communicator communicator = new Communicator(multicastGroup, multicastPort, multicastIP, unicastPortDiscovery);
        DiscoveryClient discoveryClient = new DiscoveryClient(name, IPAddress, namingPort, unicastPortDiscovery);

        System.out.println("<---> " + name + " Instantiated with IP " + IPAddress + " <--->");
        discoveryClient.bootstrap();
        NamingClient.setBaseUrl(discoveryClient.getBaseUrl());
        NamingClient.setName(name);
    }

    public static void main(String[] args) {
        // Run Client
        context = SpringApplication.run(ClientApplication.class, args);
    }
}