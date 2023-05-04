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
import java.util.ArrayList;
import java.util.Arrays;
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

        FileOutputStream os;
        try {
            os = new FileOutputStream((String) jo.get("name"));
        } catch (FileNotFoundException e) {
            System.out.println("File not found!");
            System.out.println("\tLooking for name "+jo.get("name")+ " using the method get('name') failed!");
            System.out.println("\tCheck if 'name' is the right key in the object: " + jo);
            System.out.println("\n\tException:\n\t"+e.getMessage());
            return -1;
        }

        try {
            os.write(message.getPayload());
            os.close();
        } catch (IOException e) {
            System.out.println("Failed to write to file "+jo.get("name")+"!");
            System.out.println("\n\tException:\n\t"+e.getMessage());
            return -1;
        }

        return 0;
    }

}
