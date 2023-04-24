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
    private static int unicastPortDiscovery;
    private static int previousID;
    private static int nextID;
    private static final boolean shuttingDown = false;
    private static ApplicationContext context;
    private final String name;
    private final String IPAddress;
    private final int namingPort;
    UnicastReceivingChannelAdapter adapter;
    private String baseUrl;
    // Discovery Parameters
    private String namingServerIP;
    private final String multicastIP;
    private final InetAddress multicastGroup;
    private final int multicastPort;
    private final Communicator communicator;
    private NamingClient namingClient;
    private final DiscoveryClient discoveryClient;

    public ClientApplication() throws IOException {
        name = InetAddress.getLocalHost().getHostName();
        IPAddress = InetAddress.getLocalHost().getHostAddress();

        // Choose a random IP in the 224.0.0.0 to 239.255.255.255 range (reserved for multicast)
        multicastIP = "224.0.0.5";
        multicastGroup = InetAddress.getByName(multicastIP);
        multicastPort = 4446;
        unicastPortDiscovery = 4449;
        namingPort = 8080;

        this.communicator = new Communicator(multicastGroup, multicastPort, multicastIP, unicastPortDiscovery);
        this.discoveryClient = new DiscoveryClient(name, IPAddress, namingPort, unicastPortDiscovery);

        System.out.println("<---> " + name + " Instantiated with IP " + IPAddress + " <--->");
        this.discoveryClient.bootstrap();
        NamingClient.setBaseUrl(discoveryClient.getBaseUrl());
        NamingClient.setName(name);
    }

    public static void main(String[] args) {
        // Run Client
        context = SpringApplication.run(ClientApplication.class, args);
    }

//	// -----------------------------------------------------------------------------------------------------------------
//	//                                       BOOTSTRAP, SHUTDOWN & FAILURE
//	// -----------------------------------------------------------------------------------------------------------------
//	public void bootstrap() {
//		System.out.println("<---> " + name + " Bootstrap <--->");
//		// Send multicast to other nodes and naming server
//		String data = name + "|" + IPAddress;
//		try {
//			Communicator.sendMulticast(data);
//		} catch (IOException e) {
//			System.out.println("Sending multicast failed");
//			failure();
//		}
//
//		// Listen on port 4447 for a response with the number of nodes & IP address of the naming server
//		String RxData = null;
//		try {
//			RxData = Communicator.receiveUnicast(4447);
//		} catch (IOException e) {
//			System.out.println("Receiving unicast failed");
//			failure();
//		}
//		namingServerIP = RxData.split("\\|")[0];
//		int numberOfNodes = Integer.parseInt(RxData.split("\\|")[1]);
//		System.out.println("Received answer to multicast from naming server - " + numberOfNodes + " node(s) in the network");
//
//		previousID = hashValue(name); 	// Set previousID to its own ID
//		nextID = hashValue(name); 		// Set nextID to its own ID
//		if (numberOfNodes == 1) {
//			System.out.println("<---> No other nodes present: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
//		} else {
//			System.out.println("<---> Other nodes present: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
//		}
//		// Set the baseURL for further communication with the naming server
//		baseUrl = "http://" + namingServerIP + ":" + namingPort + "/api/naming";
//	}
//
//	@PreDestroy
//	public void shutdown() {
//		System.out.println("<---> " + this.name + " Shutdown <--->");
//
//		// Set shuttingDown to true to avoid infinite failure loops
//		shuttingDown = true;
//
//		// Set the nextID value of the previous node to nextID of this node
//		if (previousID != hashValue(this.name)) {
//			System.out.println("Sending nextID to the previous node");
//			String messageToPrev = nextID + "|" + "nextID";
//			String previousIP = getIPAddress(previousID);
//			if (!previousIP.equals("NotFound")) {
//				try {
//					Communicator.sendUnicast(messageToPrev, previousIP, unicastPortDiscovery);
//				}
//				catch (IOException e) {
//					System.out.println(e.getMessage());
//				}
//			} else {
//				System.out.println("ERROR - couldn't notify previous node: IP not present in NS");
//			}
//		}
//
//		// Set the previousID value of the next node to previousID of this node
//		if (nextID != hashValue(name)) {
//			System.out.println("Sending previousID to the next node");
//			String messageToNext = previousID + "|" + "previousID";
//			String nextIP = getIPAddress(nextID);
//			if (!nextIP.equals("NotFound")) {
//				try {
//					Communicator.sendUnicast(messageToNext, nextIP, unicastPortDiscovery);
//				} catch (IOException e) {
//					System.out.println(e.getMessage());
//				}
//			} else {
//				System.out.println("ERROR - couldn't notify next node because IP is not in present in the NS");
//			}
//		}
//
//		// Delete this node from the Naming Server's database
//		deleteNode(name);
//
//		// Stop execution of Spring Boot application
//		System.out.println("<---> " + this.name + " Spring Boot Stopped <--->");
//		SpringApplication.exit(context);
//	}
//
//	public void failure() {
//		if (!shuttingDown) {
//			System.out.println("<---> " + this.name + " Failure <--->");
//			shutdown();
//		}
//	}
//
//	// -----------------------------------------------------------------------------------------------------------------
//	//                                  DISCOVERY & BOOTSTRAP ASSISTANCE METHODS
//	// -----------------------------------------------------------------------------------------------------------------
//
//	public void compareIDs(String RxData) {
//		String newNodeName = RxData.split("\\|")[0];
//		String newNodeIP = RxData.split("\\|")[1];
//
//		int newNodeID = hashValue(newNodeName);
//		int currentID = hashValue(name);
//
//		sleep(1000);    // Wait so the responses follow that of the naming server
//
//		if (currentID == nextID) {	// Test if this node is alone -> change previous and next ID to the new node
//			previousID = newNodeID;
//			nextID = newNodeID;
//			System.out.println("<---> connected to first other node - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
//			respondToMulticast(newNodeIP, currentID, "bothIDs");
//		} else if (previousID < newNodeID && newNodeID <= currentID) {	// Test if this node should become the previousID of the new node
//			previousID = newNodeID;
//			System.out.println("<---> previousID changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
//			respondToMulticast(newNodeIP, currentID, "nextID");
//		} else if (currentID <= newNodeID && newNodeID <= nextID) {	// Test if the new node should become the nextID of the new node
//			nextID = newNodeID;
//			System.out.println("<---> nextID changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
//			sleep(500);    // Wait so the responses don't collide
//			respondToMulticast(newNodeIP, currentID, "previousID");
//		}
//	}
//
//	// -----------------------------------------------------------------------------------------------------------------
//	//                                            MULTICAST LISTENER
//	// -----------------------------------------------------------------------------------------------------------------
//
//	@ServiceActivator(inputChannel = "Multicast")
//	public void multicastEvent(Message<byte[]> message) {
//		byte[] payload = message.getPayload();
//		DatagramPacket dataPacket = new DatagramPacket(payload, payload.length);
//
//		String RxData = new String(dataPacket.getData(), 0, dataPacket.getLength());
//		System.out.println(name + " - Received multicast message from other node: " + RxData);
//
//		// Use this multicast data to update your previous & next node IDs
//		compareIDs(RxData);
//	}
//
//	// -----------------------------------------------------------------------------------------------------------------
//	//                                              UNICAST LISTENER
//	// -----------------------------------------------------------------------------------------------------------------
//
//	@ServiceActivator(inputChannel = "DiscoveryUnicast")
//	public void unicastEvent(Message<byte[]> message) {
//		byte[] payload = message.getPayload();
//		DatagramPacket dataPacket = new DatagramPacket(payload, payload.length);
//
//		String RxData = new String(dataPacket.getData(), 0, dataPacket.getLength());
//		System.out.println("Received unicast message: " + RxData);
//
//		int currentID = Integer.parseInt(RxData.split("\\|")[0]);
//		String previousOrNext = RxData.split("\\|")[1];
//		if (previousOrNext.equals("bothIDs")) {				// Transmitter becomes previous & next ID
//			previousID = currentID; // Set previous ID
//			nextID = currentID;
//			System.out.println("<---> previous & next IDs changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
//		} else if (previousOrNext.equals("previousID")) {   // Transmitter becomes previous ID
//			previousID = currentID; // Set previous ID
//			System.out.println("<---> previousID changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
//		} else if (previousOrNext.equals("nextID")) {   	// Transmitter becomes next ID
//			nextID = currentID;
//			System.out.println("<---> nextID changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
//		} else {
//			System.out.println("<" + this.name + "> - ERROR - Unicast received 2nd parameter other than 'previousID' or 'nextID'");
//			failure();
//		}
//	}
//
//
//	public void respondToMulticast(String newNodeIP, int currentID, String previousOrNext) {
//		String message = currentID + "|" + previousOrNext;
//		try {
//			Communicator.sendUnicast(message, newNodeIP, unicastPortDiscovery);
//		} catch (IOException e) {
//			System.out.println("Responding to multicast failed");
//			failure();
//		}
//	}
//
//	public static Integer hashValue(String name) {
//		Integer hash = Math.abs(name.hashCode()) % 32769;
//		return hash;
//	}
//
//	public void sleep(int time) {
//		try {
//			Thread.sleep(time);
//		} catch (InterruptedException e) {
//			failure();
//		}
//	}
}