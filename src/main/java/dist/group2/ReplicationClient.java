package dist.group2;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.ip.udp.UnicastReceivingChannelAdapter;
import org.springframework.messaging.Message;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Date;


public class ReplicationClient {

    private String nodeName = InetAddress.getLocalHost().getHostName();
    private String IPAddress = InetAddress.getLocalHost().getHostAddress();
    private int fileUnicastPort;
    UnicastReceivingChannelAdapter fileAdapter;
    private Path file_path = Path.of(new File("").getAbsolutePath().concat("\\src\\files"));  //Stores the local files that need to be replicated
    private Path log_path = Path.of(new File("").getAbsolutePath().concat("\\src\\log_files"));  //Stores the local files that need to be replicated
    private WatchService file_daemon = FileSystems.getDefault().newWatchService();

    public ReplicationClient(int fileUnicastPort) throws IOException {
        this.fileUnicastPort = fileUnicastPort;
        this.file_path.register(file_daemon,
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
            writer = new BufferedWriter(new FileWriter(file_path.toString() + "\\" + fileName));
            writer.write(str);
            writer.close();
        }
    }

    public List<String> replicateFiles() throws IOException {
        List<String> localFiles = new ArrayList<>();
        File[] files = new File(file_path.toString()).listFiles();//If this pathname does not denote a directory, then listFiles() returns null.
        for (File file : files) {
            if (file.isFile()) {
                String fileName = file.getName();
                sendFile(fileName);
            }
        }
        return localFiles;
    }

    public void shutdown() throws IOException {
        // Send all files to the previous node
        // Edge case: the previous node already stores the file locally
        String previousNodeIP = NamingClient.findFile();
        List<String> localFiles = new ArrayList<>();
        File[] files = new File(file_path.toString()).listFiles();//If this pathname does not denote a directory, then listFiles() returns null.
        for (File file : files) {
            if (file.isFile()) {
                String fileName = file.getName();
                sendFile(fileName);
            }
        }

        // Transfer log file to the new node
        String log_file_path = "path";
        String destinationIP = "IP";
        sendFileToNode(log_file_path, destinationIP, false);
    }

    public void sendFile(String fileName) throws IOException {    // Send file to replicated node
        // Get IP addr of replicator node
        // Find IP address of replicator node
        String replicator_loc = NamingClient.findFile(fileName);
        sendFileToNode(fileName, replicator_loc, false);
    }

    public void sendFileToNode(String fileName, String nodeIP, boolean failure) throws IOException {    // Send file to replicated node
        // Create JSON object from File
        Path file_location = Path.of(file_path.toString() + '\\' + fileName);
        JSONObject jo = new JSONObject();
        jo.put("name", fileName);
        jo.put("data", Files.readAllBytes(file_location));
        jo.put("failure", failure);

        // Write the JSON data into a buffer
        byte[] data = jo.toString().getBytes(StandardCharsets.UTF_8);

        // Create TCP socket and
        Socket tcp_socket = new Socket(InetAddress.getByName(nodeIP), fileUnicastPort);
        OutputStream os = tcp_socket.getOutputStream();

        // Send data
        os.write(data);
        os.flush();

        tcp_socket.close();
    }

    // ----------------------------------------- FILE UNICAST RECEIVER -------------------------------------------------

    /**
     * Update a file when it has been remotely edited.
     * @param message: Message received from the Communicator
     */
    @ServiceActivator(inputChannel = "FileUnicast")
    public int fileUnicastEvent(Message<byte[]> message) {
        byte[] raw_data = message.getPayload();
        JSONObject jo;
        try {
            JSONParser parser = new JSONParser();
            jo = (JSONObject) parser.parse(raw_data);
        } catch (ParseException e) {
            System.out.println("Received message but failed to parse data!");
            System.out.println("\tRaw data received: " + Arrays.toString(raw_data));
            System.out.println("\n\tException: \n\t"+e.getMessage());
            return -1;
        }

        boolean failure = (boolean) jo.get("failure");
        if (failure) {
            // Joppe shit
            return 0;
        }

        FileOutputStream os_file;
        try {
            os_file = new FileOutputStream(file_path.toString() + '\\' + jo.get("name"));
        } catch (FileNotFoundException e) {
            System.out.println("File not found!");
            System.out.println("\tLooking for name "+jo.get("name")+ " using the method get('name') failed!");
            System.out.println("\tCheck if 'name' is the right key in the object: " + jo);
            System.out.println("\n\tException:\n\t"+e.getMessage());
            return -1;
        }

        try {
            os_file.write(message.getPayload());
            os_file.close();
        } catch (IOException e) {
            System.out.println("Failed to write to file "+jo.get("name")+"!");
            System.out.println("\n\tException:\n\t"+e.getMessage());
            return -1;
        }


        FileOutputStream os_log;
        try {
            os_log = new FileOutputStream(log_path.toString() + '\\' + jo.get("name") + ".log", true);
        } catch (FileNotFoundException e) {
            System.out.println("Log file not found!");
            System.out.println("\tLooking for name "+jo.get("name")+ ".log using the method get('name') failed!");
            System.out.println("\tCheck if 'name' is the right key in the object: " + jo);
            System.out.println("\n\tException:\n\t"+e.getMessage());
            return -2;
        }

        try {
            // Get current timestamp
            Date date = new Date(System.currentTimeMillis());
            String formatted_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);

            // Create the content of the file
            String text = "Node " + nodeName + " with IP " + IPAddress + " became the file owner on: " + formatted_date;

            // Write the file
            os_log.write(text.getBytes());
            os_log.close();
        } catch (IOException e) {
            System.out.println("Failed to write to log file "+jo.get("name")+".log!");
            System.out.println("\n\tException:\n\t"+e.getMessage());
            return -2;
        }

        return 0;
    }

}
