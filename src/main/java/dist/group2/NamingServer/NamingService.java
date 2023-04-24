package dist.group2.NamingServer;

import jakarta.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.ip.udp.MulticastReceivingChannelAdapter;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.*;

import static dist.group2.NamingServer.JsonHelper.convertMapToJson;

@Service
public class NamingService {
    private Map<Integer, String> repository;
    private String multicastGroup = "224.0.0.5";

    @Bean
    public MulticastReceivingChannelAdapter multicastReceiver(DatagramSocket socket) {
        MulticastReceivingChannelAdapter adapter = new MulticastReceivingChannelAdapter(multicastGroup, 4446);
        adapter.setOutputChannelName("Multicast");
        adapter.setSocket(socket);
        return adapter;
    }

    @Bean
    public DatagramSocket datagramSocket() throws IOException {
        MulticastSocket socket = new MulticastSocket(4446);
        InetAddress group = InetAddress.getByName(multicastGroup);
        socket.joinGroup(group);
        return socket;
    }

    @ServiceActivator(inputChannel = "Multicast")
    public void multicastEvent(Message<byte[]> message) throws IOException {
        byte[] payload = message.getPayload();
        DatagramPacket dataPacket = new DatagramPacket(payload, payload.length);

        String RxData = new String(dataPacket.getData(), 0, dataPacket.getLength());
        String nodeName = RxData.split("\\|")[0];
        String IPAddress = RxData.split("\\|")[1];
        System.out.println("Received multicast message: " + RxData + InetAddress.getLocalHost().getHostAddress());

        // Add node to repository
        Map<String, String> node = new HashMap<>();
        node.put("nodeName", nodeName);
        node.put("IPAddress", IPAddress);
        addNode(node);

        // Respond to Multicast
        respondToMulticast(IPAddress);
    }

    @Autowired
    public NamingService() {
        // repository = convertJsonToMap();
        repository = new TreeMap<>();
    }

    public Integer hashValue(String name) {
        Integer hash = Math.abs(name.hashCode()) % 32769;
        return hash;
    }

    public void respondToMulticast(String nodeIP) throws IOException {
        System.out.println("<---> Send response to multicast of node " + nodeIP + " <--->");

        // Prepare response packet
        int numberOfNodes = repository.size();
        String namingServerIP = InetAddress.getLocalHost().getHostAddress();
        String response = namingServerIP + "|" + numberOfNodes;
        byte[] Txbuffer = response.getBytes();
        DatagramPacket packet = new DatagramPacket(Txbuffer, Txbuffer.length, InetAddress.getByName(nodeIP), 4447);

        // Send response to the IP of the node on port 4447
        DatagramSocket socket = new DatagramSocket();
        socket.send(packet);
        socket.close();
    }

    public synchronized void addNode(Map<String, String> node) {
        if (repository.containsKey(hashValue(node.get("nodeName")))) {
            throw new IllegalStateException("Hash of " + node.get("nodeName") + " is already being used");
        }
        repository.put(hashValue(node.get("nodeName")), node.get("IPAddress"));
        convertMapToJson(repository);
        System.out.println("Hash " + hashValue(node.get("nodeName")) + " of node " + node.get("nodeName") + " is added to the database");
    }

    public synchronized void deleteNode(String nodeName) {
        if (!repository.containsKey(hashValue(nodeName))) {
            throw new IllegalStateException("There is no node with name" + nodeName);
        }
        repository.remove(hashValue(nodeName));
        convertMapToJson(repository);
        System.out.println("Hash of " + nodeName + " is removed from the database");
    }

    @Transactional
    public synchronized String findFile(String fileName) {
        int fileHash = this.hashValue(fileName);
        Set<Integer> hashes = repository.keySet();

        if (hashes.isEmpty()) {
            throw new IllegalStateException("There is no node in the database!");
        } else {
            List<Integer> smallerHashes = new ArrayList();
            Iterator var5 = hashes.iterator();

            while(var5.hasNext()) {
                Integer hash = (Integer)var5.next();
                if (hash < fileHash) {
                    smallerHashes.add(hash);
                }
            }

            if (smallerHashes.isEmpty()) {
                return repository.get(Collections.max(hashes));
            } else {
                return repository.get(Collections.max(smallerHashes));
            }
        }
    }

    @Transactional
    public synchronized String getIPAddress(int nodeID) {
        System.out.println("Request IP of node with ID " + nodeID);
        String IPAddress = repository.getOrDefault(nodeID, "NotFound");
        if (IPAddress.equals("NotFound")) {
            System.out.println("There is no node with ID " + nodeID + " in the repository");
        }
        return IPAddress;
    }
}