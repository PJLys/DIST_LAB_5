package dist.group2;

import jakarta.annotation.PreDestroy;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.ip.udp.MulticastReceivingChannelAdapter;
import org.springframework.integration.ip.udp.UnicastReceivingChannelAdapter;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class Communicator {
    static MulticastSocket multicastSocket;
    static String multicastIP;
    static int multicastPort;
    static InetAddress multicastGroup;
    static UnicastReceivingChannelAdapter adapter;
    static int unicastReceivePortDiscovery;

    public Communicator(InetAddress multicastGroup, int multicastPort, String multicastIP, int unicastReceivePortDiscovery) throws IOException {
        Communicator.multicastIP = multicastIP;
        Communicator.multicastGroup = multicastGroup;
        Communicator.multicastPort = multicastPort;
        Communicator.multicastSocket = new MulticastSocket();
        Communicator.unicastReceivePortDiscovery = unicastReceivePortDiscovery;
    }

    public static String receiveUnicast(int port) throws IOException {
        // Prepare receiving socket
        byte[] RxBuffer = new byte[256];
        DatagramSocket socket = new DatagramSocket(port);
        // Prepare receiving packet
        DatagramPacket dataPacket = new DatagramPacket(RxBuffer, RxBuffer.length);
        // Wait to receive & close socket
        socket.receive(dataPacket);
        socket.close();
        // Read data from dataPacket
        String RxData = new String(dataPacket.getData(), 0, dataPacket.getLength());
        return RxData;
    }

    public static void sendMulticast(String data) throws IOException {
        byte[] Txbuffer = data.getBytes();
        DatagramPacket packet = new DatagramPacket(Txbuffer, Txbuffer.length, multicastGroup, multicastPort);
        multicastSocket.send(packet);
    }

    public static void sendUnicast(String message, String IPAddress2, int port) throws IOException {
        System.out.println("<---> Send unicast to node " + IPAddress2 + " on port " + port + " <--->");

        // Prepare response packet
        byte[] Txbuffer = message.getBytes();
        DatagramPacket packet = new DatagramPacket(Txbuffer, Txbuffer.length, InetAddress.getByName(IPAddress2), port);

        DatagramSocket socket = new DatagramSocket();

        // Send response to the IP of the node on the unicast port
        socket.send(packet);
        socket.close();
        socket.disconnect();
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
        multicastSocket = new MulticastSocket(multicastPort);
        InetAddress group = InetAddress.getByName(multicastIP);
        multicastSocket.joinGroup(group);
        return multicastSocket;
    }

    @Bean
    public UnicastReceivingChannelAdapter unicastReceiver() {
        adapter = new UnicastReceivingChannelAdapter(unicastReceivePortDiscovery);
        adapter.setOutputChannelName("DiscoveryUnicast");
        return adapter;
    }

    @PreDestroy
    public void shutdown() {
        multicastSocket.close();
    }
}
