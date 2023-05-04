package dist.group2;

import net.minidev.json.JSONObject;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.ip.udp.UnicastReceivingChannelAdapter;
import org.springframework.messaging.Message;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class ReplicationClient {
    private int fileUnicastPort;
    UnicastReceivingChannelAdapter fileAdapter;
    private Path folder_path = Path.of(new File("").getAbsolutePath().concat("\\src\\files"));  //Stores the local files that need to be replicated
    private WatchService file_daemon = FileSystems.getDefault().newWatchService();

    public ReplicationClient(int fileUnicastPort) throws IOException {
        this.fileUnicastPort = fileUnicastPort;
        this.folder_path.register(file_daemon,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_DELETE);
    }

    // Create files to store on this node
    public void addFiles() throws IOException {
        String name = InetAddress.getLocalHost().getHostName();
        // Create 3 file names to add
        ArrayList<String> fileNames = new ArrayList<>();
        fileNames.add(name + "_1");
        fileNames.add(name + "_2");
        fileNames.add(name + "_3");

        // Create the files
        String str = "Text";
        BufferedWriter writer;
        for (String fileName : fileNames) {
            writer = new BufferedWriter(new FileWriter(folder_path.toString() + "\\" + fileName));
            writer.write(str);
            writer.close();
        }
    }

    public List<String> replicateFiles() throws IOException {
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

    public void sendFile(String fileName) throws IOException {    // Send file to replicated node
        // Get IP addr of replicator node
        // Find IP address of replicator node
        String replicator_loc = NamingClient.findFile(fileName);

        // Create JSON object from File
        Path file_path = Path.of(folder_path.toString() + '\\' + fileName);
        JSONObject jo = new JSONObject();
        jo.put("name", fileName);
        jo.put("data", Files.readAllBytes(file_path));

        // Write the JSON data into a buffer
        byte[] data = jo.toString().getBytes(StandardCharsets.UTF_8);

        // Create TCP socket and
        Socket tcp_socket = new Socket(InetAddress.getByName(replicator_loc), fileUnicastPort);
        OutputStream os = tcp_socket.getOutputStream();

        // Send data
        os.write(data);
        os.flush();

        tcp_socket.close();
    }

    // ----------------------------------------- FILE UNICAST RECEIVER -------------------------------------------------
    @Bean
    public UnicastReceivingChannelAdapter fileUnicastReceiver() {
        fileAdapter = new UnicastReceivingChannelAdapter(fileUnicastPort);
        fileAdapter.setOutputChannelName("FileUnicast");
        return fileAdapter;
    }

    public UnicastReceivingChannelAdapter serverUnicastReceiver() {
        UnicastReceivingChannelAdapter adapter = new UnicastReceivingChannelAdapter(fileUnicastPort);
        adapter.setOutputChannelName("FileUnicast");
        return adapter;
    }

    @ServiceActivator(inputChannel = "FileUnicast")
    public void serverUnicastEvent(Message<byte[]> message) {
        // Read the length of the JSON data as a 4-byte integer in network byte order
        DataInputStream dis = new DataInputStream(clientSocket.getInputStream()); int jsonLength = Integer.reverseBytes(dis.readInt());
        // Read the JSON data into a byte array
        byte[] jsonBytes = new byte[jsonLength]; dis.readFully(jsonBytes);
        // Convert the JSON data to a string
        String json = new String(jsonBytes, StandardCharsets.UTF_8);
        // Parse the JSON data
        JSONObject jo = new JSONObject(json);
        // Extract the file name and data from the JSON object
        String fileName = jo.getString("name"); byte[] fileData = jo.getBytes("data");
        // Write the file data to disk
        Path file_path = Path.of(folder_path.toString() + '\\' + fileName); Files.write(file_path, fileData);
        // Close the client socket and server socket
        clientSocket.close(); serverSocket.close();





        try {
            byte[] payload = message.getPayload();
            FileOutputStream outputStream = new FileOutputStream("file.txt", true); // true for append mode
            outputStream.write(payload);
            outputStream.close();
            System.out.println("Bytes appended to file successfully.");
        } catch (IOException e) {
            System.out.println("Error appending bytes to file: " + e.getMessage());
        }
    }


}
