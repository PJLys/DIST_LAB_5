package dist.group2;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.hibernate.cfg.NotYetImplementedException;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.ip.udp.UnicastReceivingChannelAdapter;
import org.springframework.messaging.Message;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.rmi.UnexpectedException;
import java.text.SimpleDateFormat;
import java.util.*;


public class ReplicationClient implements Runnable{
    private final int fileUnicastPort;

    private String nodeName = InetAddress.getLocalHost().getHostName();
    private String IPAddress = InetAddress.getLocalHost().getHostAddress();UnicastReceivingChannelAdapter fileAdapter;
    WatchService file_daemon = FileSystems.getDefault().newWatchService();
    private final Path local_file_path = Path.of(new File("").getAbsolutePath().concat("\\src\\local_files"));  //Stores the local files that need to be replicated
    private final Path replicated_file_path = Path.of(new File("").getAbsolutePath().concat("\\src\\replicated_files"));  //Stores the local files that need to be replicated

    private final Path log_path = Path.of(new File("").getAbsolutePath().concat("\\src\\log_files"));  //Stores the local files that need to be replicated

    public ReplicationClient(int fileUnicastPort) throws IOException {
        this.fileUnicastPort = fileUnicastPort;
        this.local_file_path.register(file_daemon,
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
            writer = new BufferedWriter(new FileWriter(local_file_path.toString() + "\\" + fileName));
            writer.write(str);
            writer.close();
        }
    }

    public List<String> replicateFiles() throws IOException {
        List<String> localFiles = new ArrayList<>();
        File[] files = new File(local_file_path.toString()).listFiles();//If this pathname does not denote a directory, then listFiles() returns null.
        assert files != null;
        for (File file : files) {
            if (file.isFile()) {
                String fileName = file.getName();
                sendFile( local_file_path.toString() + '\\' + fileName, "init");
            }
        }
        return localFiles;
    }

    public void shutdown() throws IOException {
        // Send all files to the previous node
        // Edge case: the previous node already stores the file locally
        String previousNodeIP = NamingClient.findFile();
        List<String> localFiles = new ArrayList<>();
        File[] files = new File(file_path.toString()).listFiles(); //If this pathname does not denote a directory, then listFiles() returns null.
        for (File file : files) {
            if (file.isFile()) {
                String fileName = file.getName();
                // sendFile(fileName, "shutdown");
                String destinationIP = NamingClient.findFile(fileName);

                // Transfer log file to the new node
                if (destinationIP == IPAddress) {
                    System.out.println("Shutdown and this node is the owner of the file, send warning to previous node");
                    sendFileToNode(log_path.toString() +  + '\\' + fileName, previousNodeIP, "warning");
                } else {
                    System.out.println("Shutdown and this node is not the owner of the file, send warning has to owner of the file");
                    // Transfer log file to the new node
                    sendFileToNode(log_path.toString() +  + '\\' + fileName, destinationIP, "warning");
                }
            }
        }
    }

    public void sendFile(String filePath, String extra_message) throws IOException {    // Send file to replicated node
        // Get IP addr of replicator node
        // Find IP address of replicator node
        String replicator_loc = NamingClient.findFile(Path.of(filePath).getFileName().toString());
        sendFileToNode(filePath, replicator_loc, extra_message);
    }

    public void sendFileToNode(String filePath, String nodeIP, String extra_message) throws IOException {    // Send file to replicated node
        // Create JSON object from Filegit c
        Path file_location = Path.of(filePath);
        JSONObject jo = new JSONObject();
        jo.put("name", file_location.getFileName().toString());
        if (!Objects.equals(extra_message, "warning")) {
            jo.put("data", Files.readAllBytes(file_location));
        }
        jo.put("extra_message", extra_message);

        // Write the JSON data into a buffer
        byte[] data = jo.toString().getBytes(StandardCharsets.UTF_8);

        // Create TCP socket and output stream
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
    public int fileUnicastEvent(Message<byte[]> message) throws IOException {
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

        String fileName = (String) jo.get("extra_message");
        String extra_message = (String) jo.get("extra_message");
        String file_path = log_path.toString() + '\\' + fileName;
        if (Objects.equals(extra_message, "warning")) {
            System.out.println("I am the owner of " + fileName + " and got a warning.");
            if (wasDownloaded(file_path)) {
                // Update the log file with the download locations
                System.out.println(fileName + " contains a download of the file, update the log file");

                FileOutputStream os_log;
                try {
                    os_log = new FileOutputStream(log_path.toString() + '\\' + fileName + ".log", true);
                } catch (FileNotFoundException e) {
                    System.out.println("Log file not found!");
                    System.out.println("\tLooking for name "+fileName+ ".log using the method get('name') failed!");
                    System.out.println("\tCheck if 'name' is the right key in the object: " + jo);
                    System.out.println("\n\tException:\n\t"+e.getMessage());
                    return -2;
                }

                try {
                    // Get current timestamp
                    Date date = new Date(System.currentTimeMillis());
                    String formatted_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);

                    // Create the content of the file
                    String update_text = "Update of this log file happened due to change of owner at: " + formatted_date + "\n";

                    // Write the text that indicates that the log is update
                    os_log.write(update_text.getBytes());

                    // Append the lines of the old log file
                    os_log.write(message.getPayload());

                    // Close the output stream
                    os_log.close();
                } catch (IOException e) {
                    System.out.println("Failed to write to log file "+fileName+".log!");
                    System.out.println("\n\tException:\n\t"+e.getMessage());
                    return -2;
                }
            } else {
                // This log file can be removed since the file was never downloaded
                System.out.println(fileName + " contains no download of the file, remove the log file");
                Files.deleteIfExists(Path.of(file_path));
            }

            return 0;
        }

        FileOutputStream os_file;
        try {
            os_file = new FileOutputStream(replicated_file_path.toString() + '\\' + fileName);
        } catch (FileNotFoundException e) {
            System.out.println("File not found!");
            System.out.println("\tLooking for name "+fileName+ " using the method get('name') failed!");
            System.out.println("\tCheck if 'name' is the right key in the object: " + jo);
            System.out.println("\n\tException:\n\t"+e.getMessage());
            return -1;
        }

        try {
            os_file.write(message.getPayload());
            os_file.close();
        } catch (IOException e) {
            System.out.println("Failed to write to file "+fileName+"!");
            System.out.println("\n\tException:\n\t"+e.getMessage());
            return -1;
        }


        FileOutputStream os_log;
        try {
            os_log = new FileOutputStream(log_path.toString() + '\\' + fileName + ".log", true);
        } catch (FileNotFoundException e) {
            System.out.println("Log file not found!");
            System.out.println("\tLooking for name "+fileName+ ".log using the method get('name') failed!");
            System.out.println("\tCheck if 'name' is the right key in the object: " + jo);
            System.out.println("\n\tException:\n\t"+e.getMessage());
            return -2;
        }

        try {
            // Get current timestamp
            Date date = new Date(System.currentTimeMillis());
            String formatted_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);

            // Create the content of the file
            String text = "Node " + nodeName + " with IP " + IPAddress + " became the file owner on: " + formatted_date + "\n";

            // Write the file
            os_log.write(text.getBytes());
            os_log.close();
        } catch (IOException e) {
            System.out.println("Failed to write to log file "+fileName+".log!");
            System.out.println("\n\tException:\n\t"+e.getMessage());
            return -2;
        }

        return 0;
    }

    public boolean wasDownloaded(String file_path) {
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(file_path));
            String line = reader.readLine();

            while (line != null) {
                System.out.println(line);
                // read next line
                line = reader.readLine();
                if (line.contains("ownload")) { // Check if the line contains a download
                    return true;
                }
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new NotYetImplementedException("Danku Ibe, wij houden van u! ;)");
        }
        return false;
    }

    /**
     * This method is used when a modify or create event is detected
     * @param event detected WatchEvent
     * @return error code
     */
    public int event_handler(WatchEvent<?> event) {
        Path filename = (Path) event.context();
        Path filepath = local_file_path.resolve(filename);
        System.out.println("File created: "+ filepath);
        System.out.println("Sending replication request");
        try {
            sendFile( local_file_path.toString() + '\\' + filename.toString(), event.kind().toString());
        } catch (IOException e) {
            System.out.println("Failed to send file!");
            System.out.println("\nException: \n\t");
            System.out.println(e.getMessage());
            return -1;
        }
        return 0;
    }

    public void run() {
        WatchKey watchKey;
        while (true) {
            watchKey = file_daemon.poll(); // Could use .take() but this blocks the loop
            if (watchKey!= null) {
                for (WatchEvent<?> event:watchKey.pollEvents()){
                    event_handler(event);
                }
                watchKey.reset();
            }
        }
    }

}
