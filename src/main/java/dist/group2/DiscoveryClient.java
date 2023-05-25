package dist.group2;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.file.Path;
import java.util.Arrays;

@Service
public class DiscoveryClient {
    private static int previousID;
    private int nextID;
    private static String name;
    private String IPAddress;
    private int namingPort;
    private int unicastPort;
    private String baseUrl;
    private static boolean shuttingDown = false;

    public void init(String name, String IPAddress, int unicastPort, int namingPort) {
        this.name = name;
        this.IPAddress = IPAddress;
        this.baseUrl = null;
        this.namingPort = namingPort;
        this.unicastPort = unicastPort;
        this.previousID = hashValue(name);    // Set previousID to its own ID
        this.nextID = hashValue(name);        // Set nextID to its own ID
    }

    public static int getPreviousID() {
        return previousID;
    }

    public static Integer hashValue(String name) {
        return Math.abs(name.hashCode()) % 32769;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void bootstrap() {
        System.out.println("<---> " + name + " Bootstrap <--->");
        // Send multicast to other nodes and naming server
        String data = name + "|" + IPAddress;
        System.out.println("<---> " + name + " Discovery Multicast Sending <--->");
        try {
            Communicator.sendMulticast(data);
        }
        catch (IOException e) {
            System.out.println("Failed to send multicast");
            failure();
        }

        // Listen for a response with the number of nodes & IP address of the naming server
        int receiveUnicastPort = 4447;
        System.out.println("<---> Waiting for unicast response from NS to multicast of node " + IPAddress + " <--->");
        String rxData = null;
        try {
            rxData = Communicator.receiveUnicast(receiveUnicastPort);
        }
        catch (IOException e) {
            System.out.println(Arrays.toString(e.getStackTrace()));
            failure();
        }

        String namingServerIP = rxData.split("\\|")[0];
        int numberOfNodes = Integer.parseInt(rxData.split("\\|")[1]);
        System.out.println("Received answer to multicast from naming server - " + numberOfNodes + " node(s) in the network");

        if (numberOfNodes == 1) {
            System.out.println("<---> No other nodes present: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
        } else {
            System.out.println("<---> Other nodes present: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
        }
        // Set the baseURL for further communication with the naming server
        this.baseUrl = "http://" + namingServerIP + ":" + this.namingPort + "/api/naming";
    }

    @PreDestroy
    private void shutdown() {
        System.out.println("<---> " + this.name + " Shutdown <--->");

        // Set shuttingDown to true to avoid infinite failure loops
        shuttingDown = true;

        // Set the nextID value of the previous node to nextID of this node
        if (previousID != hashValue(this.name)) {
            System.out.println("Sending nextID to the previous node");
            String messageToPrev = nextID + "|" + "nextID";
            String previousIP = NamingClient.getIPAddress(previousID);
            if (!previousIP.equals("NotFound")) {
                try {
                    Communicator.sendUnicast(messageToPrev, previousIP, unicastPort);
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            } else {
                System.out.println("ERROR - couldn't notify previous node: IP not present in NS");
            }
        }

        // Set the previousID value of the next node to previousID of this node
        if (nextID != hashValue(name)) {
            System.out.println("Sending previousID to the next node");
            String messageToNext = previousID + "|" + "previousID";
            String nextIP = NamingClient.getIPAddress(nextID);
            if (!nextIP.equals("NotFound")) {
                try {
                    Communicator.sendUnicast(messageToNext, nextIP, unicastPort);
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            } else {
                System.out.println("ERROR - couldn't notify next node because IP is not in present in the NS");
            }
        }

        // Delete this node from the Naming Server's database
        NamingClient.deleteNode(name);
    }

    public static void failure() {
        if (!shuttingDown) {
            System.out.println("<---> " + DiscoveryClient.name + " Failure <--->");
            SpringApplication.exit(ClientApplication.context);
        }
    }

    private void compareIDs(String RxData) {
        String newNodeName = RxData.split("\\|")[0];
        String newNodeIP = RxData.split("\\|")[1];

        int newNodeID = hashValue(newNodeName);
        int currentID = hashValue(name);

        sleep(1000);    // Wait so the responses follow that of the naming server

        if (currentID == nextID) {    // Test if this node is alone -> change previous and next ID to the new node
            previousID = newNodeID;
            nextID = newNodeID;
            System.out.println("<---> connected to first other node - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
            respondToMulticast(newNodeIP, currentID, "bothIDs");
        } else if (previousID < newNodeID && newNodeID <= currentID) {    // Test if this node should become the previousID of the new node
            previousID = newNodeID;
            System.out.println("<---> previousID changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
            respondToMulticast(newNodeIP, currentID, "nextID");
        } else if (currentID <= newNodeID && newNodeID <= nextID) {    // Test if the new node should become the nextID of the new node
            nextID = newNodeID;
            System.out.println("<---> nextID changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
            sleep(500);    // Wait so the responses don't collide
            respondToMulticast(newNodeIP, currentID, "previousID");
        }
    }

    @ServiceActivator(inputChannel = "Multicast")
    private void multicastEvent(Message<byte[]> message) throws IOException, InterruptedException {
        byte[] payload = message.getPayload();
        DatagramPacket dataPacket = new DatagramPacket(payload, payload.length);

        String RxData = new String(dataPacket.getData(), 0, dataPacket.getLength());
        System.out.println(name + " - Received multicast message from other node: " + RxData);

        // Use this multicast data to update your previous & next node IDs
        compareIDs(RxData);

        // ReplicationClient.getInstance().changeOwnerWhenNodeIsAdded();
    }

    @ServiceActivator(inputChannel = "DiscoveryUnicast")
    private void unicastEvent(Message<byte[]> message) {
        byte[] payload = message.getPayload();
        DatagramPacket dataPacket = new DatagramPacket(payload, payload.length);

        String RxData = new String(dataPacket.getData(), 0, dataPacket.getLength());
        System.out.println("Received unicast message: " + RxData);

        int currentID = Integer.parseInt(RxData.split("\\|")[0]);
        String previousOrNext = RxData.split("\\|")[1];
        switch (previousOrNext) {
            case "bothIDs" -> {                 // Transmitter becomes previous & next ID
                previousID = currentID; // Set previous ID
                nextID = currentID;
                System.out.println("<---> previous & next IDs changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
            }
            case "previousID" -> {    // Transmitter becomes previous ID
                previousID = currentID; // Set previous ID
                System.out.println("<---> previousID changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
            }
            case "nextID" -> {     // Transmitter becomes next ID
                nextID = currentID;
                System.out.println("<---> nextID changed - previousID: " + previousID + ", thisID: " + hashValue(name) + ", nextID: " + nextID + " <--->");
            }
            default -> {
                System.out.println("<" + this.name + "> - ERROR - Unicast received 2nd parameter other than 'previousID' or 'nextID'");
                failure();
            }
        }
    }

    private void respondToMulticast(String newNodeIP, int currentID, String previousOrNext) {
        String message = currentID + "|" + previousOrNext;
        try {
            Communicator.sendUnicast(message, newNodeIP, unicastPort);
            System.out.println("<---> Send response to multicast of node " + newNodeIP + " <--->");
        } catch (IOException e) {
            System.out.println("Responding to multicast failed");
            failure();
        }
    }

    private void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            failure();
        }
    }
}

