package dist.group2.NamingServer;

import jakarta.annotation.PreDestroy;
import jakarta.persistence.criteria.CriteriaBuilder;
import jdk.jshell.spi.ExecutionControl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.ip.udp.MulticastReceivingChannelAdapter;
import org.springframework.integration.ip.udp.UnicastReceivingChannelAdapter;
import org.springframework.messaging.Message;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@SpringBootApplication
public class ClientApplication {
	private final String name = InetAddress.getLocalHost().getHostName();;
	private final String IPAddress = InetAddress.getLocalHost().getHostAddress();
	private final int namingPort = 8080;
	private final RestTemplate restTemplate = new RestTemplate();
	private String baseUrl;

	// Discovery Parameters
	private String namingServerIP;
	private String multicastIP = "224.0.0.5";
	private final int multicastPort = 4446;
	private int unicastPort= 4449;
	private boolean shuttingDown=false;
	private MulticastSocket multicastSocket=new MulticastSocket();

	private static ApplicationContext context;
	UnicastReceivingChannelAdapter adapter;

	// Set previous & next ID to itself (even if there are other nodes, the IDs will be updated later on)
	private int previousID = hashValue(name);
	private int nextID = hashValue(name);

	// Replication parameters
	private int serverUnicastPort = 4451;
	private Path folder_path = Path.of(new File("").getAbsolutePath().concat("\\src\\files"));
	//Stores the local files that need to be replicated
	private WatchService file_daemon = FileSystems.getDefault().newWatchService();


	public static void main(String[] args) {
		// Run Client
		context = SpringApplication.run(ClientApplication.class, args);
	}

	public ClientApplication() throws IOException {
		System.out.println("<---> " + name + " Instantiated with IP " + IPAddress + " <--->");
		addFiles();
		folder_path.register(file_daemon,
				StandardWatchEventKinds.ENTRY_CREATE,
				StandardWatchEventKinds.ENTRY_MODIFY,
				StandardWatchEventKinds.ENTRY_DELETE);

		sleep(100);
		bootstrap();
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                    		  LAB 5 - Replication
	// -----------------------------------------------------------------------------------------------------------------
	// Create files to store on this node
	public void addFiles() throws IOException {
		// Path to store the files in
		String path = folder_path.toString();

		// Create 3 file names to add
		ArrayList<String> fileNames = new ArrayList<>();
		fileNames.add(name + "_1");
		fileNames.add(name + "_2");
		fileNames.add(name + "_3");

		// Create the files
		String str = "Text";
		BufferedWriter writer = null;
		for (String fileName : fileNames) {
			writer = new BufferedWriter(new FileWriter(filePath + "\\" + fileName));
			writer.write(str);
		}

		writer.close();
	}

	public List<String> replicateFiles(){      //get's the list of files
		List<String> localFiles = new ArrayList<>();
		File[] files = new File(folder_path.toString()).listFiles();//If this pathname does not denote a directory, then listFiles() returns null.
		for (File file : files) {
			if (file.isFile()) {
				String fileName = file.getName();
				sendFile(fileName);
			}
		}
		return localFiles;
	}
	
	public void sendFile(String fileName) {	// Send file to replicated node
		String fileLocation = findFile(fileName);
		System.out.println("Received location reply from server - " + fileName + " is located at " + fileLocation);
		System.out.println("Send file to " + fileLocation);

		//
	}

	// ----------------------------------------------------------------------------------------------
	@Bean
	public UnicastReceivingChannelAdapter serverUnicastReceiver() {
		adapter = new UnicastReceivingChannelAdapter(unicastPort);
		adapter.setOutputChannelName("ServerUnicast");
		return adapter;
	}

	@ServiceActivator(inputChannel = "ServerUnicast")
	public void serverUnicastEvent(Message<byte[]> message) {
		byte[] payload = message.getPayload();
		DatagramPacket dataPacket = new DatagramPacket(payload, payload.length);

		String RxData = new String(dataPacket.getData(), 0, dataPacket.getLength());
		System.out.println("Received unicast reply from server with : " + RxData);

		int currentID = Integer.parseInt(RxData.split("\\|")[0]);


		System.out.println("Send file to " + );
	}


	// -----------------------------------------------------------------------------------------------------------------
	//                                       BOOTSTRAP, SHUTDOWN & FAILURE
	// -----------------------------------------------------------------------------------------------------------------
	public void bootstrap() {
		System.out.println("<---> " + name + " Bootstrap <--->");
		try {
			// Send multicast to other nodes and naming server
			sendMulticast();

			// Listen on port 4447 for a response with the number of nodes & IP address of the naming server
			String RxData = receiveUnicast(4447);
			namingServerIP = RxData.split("\\|")[0];
			int numberOfNodes = Integer.parseInt(RxData.split("\\|")[1]);
			System.out.println("Received answer to multicast from naming server - " + numberOfNodes + " node(s) in the network");


			if (numberOfNodes == 1) {
				previousID = hashValue(name); 	// Set previousID to its own ID
				nextID = hashValue(name); 		// Set nextID to its own ID
				System.out.println("<---> No other nodes present: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
			} else {
				previousID = hashValue(name); 	// Set previousID to its own ID
				nextID = hashValue(name); 		// Set nextID to its own ID
				System.out.println("<---> Other nodes present: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
			}

			// Set the baseURL for further communication with the naming server
			baseUrl = "http://" + namingServerIP + ":" + namingPort + "/api/naming";
		} catch (Exception e) {
			System.out.println("\t"+e.getMessage());
		}
	}

	@PreDestroy
	public void shutdown() {
		System.out.println("<---> " + name + " Shutdown <--->");

		// Set shuttingDown to true to avoid infinite failure loops
		shuttingDown = true;

		// Set the nextID value of the previous node to nextID of this node
		if (previousID != hashValue(name)) {
			System.out.println("Sending nextID to the previous node");
			String messageToPrev = nextID + "|" + "nextID";
			String previousIP = getIPAddress(previousID);
			if (!previousIP.equals("NotFound")) {
				sendUnicast(messageToPrev, previousIP, unicastPort);
			} else {
				System.out.println("ERROR - couldn't notify previous node: IP not present in NS");
			}
		}

		// Set the previousID value of the next node to previousID of this node
		if (nextID != hashValue(name)) {
			System.out.println("Sending previousID to the next node");
			String messageToNext = previousID + "|" + "previousID";
			String nextIP = getIPAddress(nextID);
			if (!nextIP.equals("NotFound")) {
				sendUnicast(messageToNext, nextIP, unicastPort);
			} else {
				System.out.println("ERROR - couldn't notify next node because IP is not in present in the NS");
			}
		}

		// Delete this node from the Naming Server's database
		deleteNode(name);

		// Stop execution of Spring Boot application
		System.out.println("<---> " + name + " Spring Boot Stopped <--->");
		multicastSocket.close();
		SpringApplication.exit(context);
	}

	public void failure() {
		if (!shuttingDown) {
			System.out.println("<---> " + name + " Failure <--->");
			shutdown();
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                  DISCOVERY & BOOTSTRAP ASSISTANCE METHODS
	// -----------------------------------------------------------------------------------------------------------------
	public void sendMulticast() {
		try {
			System.out.println("<---> " + name + " Discovery Multicast Sending <--->");

			String data = name + "|" + IPAddress;
			byte[] Txbuffer = data.getBytes();
			DatagramPacket packet = new DatagramPacket(Txbuffer, Txbuffer.length, InetAddress.getByName(multicastIP), multicastPort);

			multicastSocket.send(packet);
		} catch (IOException e) {
			System.out.println("<" + this.name + "> - ERROR - Failed to send multicast - " + e);
			failure();
		}
	}

	public void compareIDs(String RxData) {
		String newNodeName = RxData.split("\\|")[0];
		String newNodeIP = RxData.split("\\|")[1];

		int newNodeID = hashValue(newNodeName);
		int currentID = hashValue(name);

		sleep(1000);    // Wait so the responses follow that of the naming server

		if (currentID == nextID) {	// Test if this node is alone -> change previous and next ID to the new node
			previousID = newNodeID;
			nextID = newNodeID;
			System.out.println("<---> connected to first other node - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
			respondToMulticast(newNodeIP, currentID, "bothIDs");
		} else if (previousID < newNodeID && newNodeID <= currentID) {	// Test if this node should become the previousID of the new node
			previousID = newNodeID;
			System.out.println("<---> previousID changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
			respondToMulticast(newNodeIP, currentID, "nextID");
		} else if (currentID <= newNodeID && newNodeID <= nextID) {	// Test if the new node should become the nextID of the new node
			nextID = newNodeID;
			System.out.println("<---> nextID changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
			sleep(500);    // Wait so the responses don't collide
			respondToMulticast(newNodeIP, currentID, "previousID");
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                            MULTICAST LISTENER
	// -----------------------------------------------------------------------------------------------------------------
	@Bean
	public MulticastReceivingChannelAdapter multicastReceiver(DatagramSocket socket) {
		MulticastReceivingChannelAdapter adapter = new MulticastReceivingChannelAdapter(multicastIP, 4446);
		adapter.setOutputChannelName("Multicast");
		adapter.setSocket(socket);
		return adapter;
	}

	@Bean
	public DatagramSocket datagramSocket() throws IOException {
		try {
			multicastSocket = new MulticastSocket(multicastPort);
		} catch (Exception e) {
			System.out.println("Address already in use");
			failure();
		}
		InetAddress group = InetAddress.getByName(multicastIP);
		multicastSocket.joinGroup(group);
		return multicastSocket;
	}

	@ServiceActivator(inputChannel = "Multicast")
	public void multicastEvent(Message<byte[]> message) {
		byte[] payload = message.getPayload();
		DatagramPacket dataPacket = new DatagramPacket(payload, payload.length);

		String RxData = new String(dataPacket.getData(), 0, dataPacket.getLength());
		System.out.println(name + " - Received multicast message from other node: " + RxData);

		// Use this multicast data to update your previous & next node IDs
		compareIDs(RxData);
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                              UNICAST LISTENER
	// -----------------------------------------------------------------------------------------------------------------
	@Bean
	public UnicastReceivingChannelAdapter unicastReceiver() {
		adapter = new UnicastReceivingChannelAdapter(unicastPort);
		adapter.setOutputChannelName("Unicast");
		return adapter;
	}

	@ServiceActivator(inputChannel = "Unicast")
	public void unicastEvent(Message<byte[]> message) {
		byte[] payload = message.getPayload();
		DatagramPacket dataPacket = new DatagramPacket(payload, payload.length);

		String RxData = new String(dataPacket.getData(), 0, dataPacket.getLength());
		System.out.println("Received unicast message: " + RxData);

		int currentID = Integer.parseInt(RxData.split("\\|")[0]);
		String previousOrNext = RxData.split("\\|")[1];
		if (previousOrNext.equals("bothIDs")) {				// Transmitter becomes previous & next ID
			previousID = currentID; // Set previous ID
			nextID = currentID;
			System.out.println("<---> previous & next IDs changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
		} else if (previousOrNext.equals("previousID")) {   // Transmitter becomes previous ID
			previousID = currentID; // Set previous ID
			System.out.println("<---> previousID changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
		} else if (previousOrNext.equals("nextID")) {   	// Transmitter becomes next ID
			nextID = currentID;
			System.out.println("<---> nextID changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
		} else {
			System.out.println("<" + this.name + "> - ERROR - Unicast received 2nd parameter other than 'previousID' or 'nextID'");
			failure();
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                          GENERAL PURPOSE METHODS
	// -----------------------------------------------------------------------------------------------------------------
	public String receiveUnicast(int port) {
		try {
			System.out.println("<---> Waiting for unicast response from NS to multicast of node " + IPAddress + " <--->");

			// Prepare receiving socket
			byte[] RxBuffer = new byte[256];
			DatagramSocket socket = null;
			try {
				socket = new DatagramSocket(port);
			} catch (Exception e) {
				System.out.println("Address already in use");
				failure();
			}

			// Prepare receiving packet
			DatagramPacket dataPacket = new DatagramPacket(RxBuffer, RxBuffer.length);

			// Wait to receive & close socket
			socket.receive(dataPacket);
			socket.close();

			// Read data from dataPacket
			String RxData = new String(dataPacket.getData(), 0, dataPacket.getLength());
			return RxData;
		} catch (IOException e) {
			System.out.println("<" + this.name + "> - ERROR - Failed to receive unicast - " + e);
			failure();
			throw new IllegalStateException("Client has failed and should have been stopped by now");
		}
	}

	public void sendUnicast(String message, String IPAddress2, int port) {
		try {
			System.out.println("<---> Send unicast to node " + IPAddress2 + " on port " + port + " <--->");

			// Prepare response packet
			byte[] Txbuffer = message.getBytes();
			DatagramPacket packet = new DatagramPacket(Txbuffer, Txbuffer.length, InetAddress.getByName(IPAddress2), port);

			// Create socket on the unicast port (without conflicting with UnicastListener which uses the same port)
			adapter.stop();
			sleep(500);
			DatagramSocket socket = null;
			try {
				// Acquire the lock before creating the DatagramSocket
				socket = new DatagramSocket();
			} catch (Exception e) {
				System.out.println("Address already in use");
				failure();
			}

			// Send response to the IP of the node on the unicast port
			if (socket != null) {
				socket.send(packet);
				socket.close();
				socket.disconnect();
			}
			sleep(500);
			adapter.start();
		} catch (IOException e) {
			System.out.println("<" + this.name + "> - ERROR - Failed to send unicast - " + e);
			failure();
			throw new IllegalStateException("Client has failed and should have been stopped by now");
		}
	}

	public void respondToMulticast(String newNodeIP, int currentID, String previousOrNext) {
		String message = currentID + "|" + previousOrNext;
		sendUnicast(message, newNodeIP, unicastPort);
	}

	public Integer hashValue(String name) {
		Integer hash = Math.abs(name.hashCode()) % 32769;
		return hash;
	}

	public void sleep(int time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			failure();
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                        REST METHODS (NAMING SERVER)
	// -----------------------------------------------------------------------------------------------------------------
	@PostMapping
	public void addNode(String nodeName, String IPAddress) {
		String url = baseUrl;

		Map<String, String> requestBody = new HashMap<>();
		requestBody.put("nodeName", nodeName);
		requestBody.put("IPAddress", IPAddress);
		try {
			System.out.println("<" + this.name + "> - Add node with name " + nodeName + " and IP address " + IPAddress);
			restTemplate.postForObject(url, requestBody, Void.class);
		} catch(Exception e) {
			System.out.println("<" + this.name + "> - ERROR - Failed to add " + nodeName + ", hash collision occurred - " + e);
			failure();
		}
	}

	@DeleteMapping
	public void deleteNode(String nodeName) {
		String url = baseUrl + "/" + nodeName;
		try {
			restTemplate.delete(url);
			System.out.println("<" + this.name + "> - Deleted node with name " + nodeName);
		} catch(Exception e) {
			System.out.println("<" + this.name + "> - ERROR - Failed to delete " + nodeName + " - " + e);
			// Avoid calling failure if the node is already shutting down (to prevent infinite loops)
			if(!shuttingDown) {
				failure();
			}
		}
	}

	@GetMapping
	public String findFile(String fileName) {
		String url = baseUrl + "?fileName=" + fileName;
		try {
			String IPAddress = restTemplate.getForObject(url, String.class);
			System.out.println("<" + this.name + "> - " + fileName + " is stored at IPAddress " + IPAddress);
			return IPAddress;
		} catch(Exception e) {
			System.out.println("<" + this.name + "> - ERROR - Failed to find " + fileName + ", no nodes in database - " + e);
			failure();
			return null;
		}
	}

	@GetMapping
	public String getIPAddress(int nodeID) {
		String url = baseUrl + "/translate" + "?nodeID=" + nodeID;
		try {
			String IPAddress = restTemplate.getForObject(url, String.class);
			System.out.println("<" + this.name + "> - Node with ID " + nodeID + " has IPAddress " + IPAddress);
			return IPAddress;
		} catch(Exception e) {
			System.out.println("<" + this.name + "> - ERROR - Failed to find IPAddress of node with ID " + nodeID + " - " + e);
			failure();
			return "NotFound";
		}
	}
}