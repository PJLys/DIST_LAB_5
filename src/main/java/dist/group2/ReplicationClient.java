package dist.group2;

import jakarta.annotation.PreDestroy;
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
    private int nodeID = DiscoveryClient.hashValue(nodeName);
    private String IPAddress = InetAddress.getLocalHost().getHostAddress();UnicastReceivingChannelAdapter fileAdapter;

    WatchService file_daemon = FileSystems.getDefault().newWatchService();
    private final Path local_file_path = Path.of(new File("").getAbsolutePath().concat("\\src\\local_files"));  //Stores the local files that need to be replicated
    private final Path replicated_file_path = Path.of(new File("").getAbsolutePath().concat("\\src\\replicated_files"));  //Stores the local files that need to be replicated
    private final Path log_path = Path.of(new File("").getAbsolutePath().concat("\\src\\log_files"));  //Stores the local files that need to be replicated

    public ReplicationClient(int fileUnicastPort) throws IOException {
        this.fileUnicastPort = fileUnicastPort;
    }

    public void setFileDirectoryWatchDog() throws IOException {

        System.out.println("before");
        System.out.println(local_file_path);

        try {
            this.local_file_path.register(file_daemon,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE);
            System.out.println("worked");
        } catch (Exception e) {
            System.out.println("Failed to send file!");
            System.out.println("\nException: \n\t");
            System.out.println(e.getMessage());
            System.out.println("worked");
        }

        try {
            Path local_file_path2 = Path.of(new File("").getAbsolutePath().concat("/src/local_files"));  //Stores the local files that need to be replicated

            local_file_path2.register(file_daemon,
                            StandardWatchEventKinds.ENTRY_CREATE,
                            StandardWatchEventKinds.ENTRY_MODIFY,
                            StandardWatchEventKinds.ENTRY_DELETE);
        } catch (Exception e) {
            System.out.println("Failed to send file!");
            System.out.println("\nException: \n\t");
            System.out.println(e.getMessage());
        }

        System.out.println("after");
    }

    /**
     * This method is used when an event is detected
     * @param event detected WatchEvent
     * @return error code
     */
    public int event_handler(WatchEvent<?> event) {
        Path filename = (Path) event.context();
        Path filepath = local_file_path.resolve(filename);
        System.out.println("File created: "+ filepath);
        System.out.println("Sending replication request");
        try {
            String filePath = local_file_path.toString() + '\\' + filename;
            String replicator_loc = NamingClient.findFile(Path.of(filePath).getFileName().toString());
            sendFileToNode(filePath, null, replicator_loc, event.kind().toString());
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
            Thread.yield();
        }
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
            System.out.println(local_file_path + "\\" + fileName);
            writer = new BufferedWriter(new FileWriter(local_file_path + "\\" + fileName));
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
                String filePath = local_file_path.toString() + '\\' + fileName;
                String replicator_loc = NamingClient.findFile(Path.of(filePath).getFileName().toString());
                sendFileToNode( filePath, null, replicator_loc, "ENTRY_CREATE");
            }
        }
        return localFiles;
    }

    @PreDestroy
    public void shutdown() throws IOException {
        System.out.println("NODE ENTERING SHUTDOWN - Local files will be removed and replicated files will be replicated.");

        // Find the IP address of the previous node
        int previousNodeID = DiscoveryClient.getPreviousID();
        String previousNodeIP = NamingClient.getIPAddress(previousNodeID);

        // If this node is the only one in the network, return from this method
        if (previousNodeID == this.nodeID) {
            System.out.println("This node is the only one in the network. No files have to be sent.");
            return;
        }

        // Get a list of the files in both directories
        File[] localFiles = new File(local_file_path.toString()).listFiles();
        File[] replicatedFiles = new File(local_file_path.toString()).listFiles();

        // Test if one of the directories cannot be found
        if (localFiles == null || replicatedFiles == null) {
            System.out.println("ERROR - One of the file directories cannot be found!");
            DiscoveryClient.failure();
        }

        // Send a warning to the owners of these files so they can delete their replicated versions
        for (File file : localFiles) {
            // Get info of the file
            String fileName = file.getName();
            String filePath = local_file_path.toString() +  + '\\' + fileName;

            // The destination is the owner of the file instead of the previous node
            String destinationIP = NamingClient.findFile(fileName);

            System.out.println("Send warning to delete file " + file.getName() + " to node " + destinationIP);

            // Warn the owner of the file to delete the replicated file
            sendFileToNode(filePath, null, destinationIP, "ENTRY_DELETE");
        }

        // Send the replicated files and their logs to the previous node which will become the new owner of the file.
        // When the previous node already stores this file locally -> send it to its previous node
        for (File file : replicatedFiles) {
            System.out.println("Replicating file " + file.getName() + " to node " + previousNodeIP);

            // Get info of the file
            String fileName = file.getName();
            String filePath = replicated_file_path.toString() +  + '\\' + fileName;
            String logPath = log_path.toString() +  + '\\' + fileName + ".log";

            // Transfer the file and its log to the previous node
            sendFileToNode(filePath, logPath, previousNodeIP, "ENTRY_SHUTDOWN_REPLICATE");
        }
    }

    public void sendFileToNode(String filePath, String logPath, String nodeIP, String extra_message) throws IOException {
        // Create JSON object from File
        JSONObject jo = new JSONObject();

        // Get the info of the file
        Path fileLocation = Path.of(filePath);
        String fileName = fileLocation.getFileName().toString();

        // Put the payload data in the JSON object
        jo.put("name", fileName);
        jo.put("extra_message", extra_message);
        jo.put("data", Files.readAllBytes(fileLocation));

        // Also include the data of the log file when necessary
        if (logPath == null) {
            jo.put("log_data", null);
        } else {
            jo.put("log_data", Files.readAllBytes(Path.of(logPath)));
        }

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

    public void retransmitFile(JSONObject json, String nodeIP) throws IOException {
        // Write the JSON data into a buffer
        byte[] data = json.toString().getBytes(StandardCharsets.UTF_8);

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

        String file_name = (String) jo.get("name");
        String extra_message = (String) jo.get("extra_message");
        String data = (String) jo.get("data");
        String log_data = (String) jo.get("log_data");

        String file_path = replicated_file_path.toString() + '\\' + file_name;
        String log_file_path = log_path.toString() + '\\' + file_name + ".log";

        // Get current timestamp
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(System.currentTimeMillis()));

        System.out.println("Received unicast of type: " + extra_message);
        if (Objects.equals(extra_message, "ENTRY_SHUTDOWN_REPLICATE")) {
            boolean fileFoundLocally = fileStoredLocally(file_name);

            if (fileFoundLocally) {
                // Find the IP address of the previous node
                int previousNodeID = DiscoveryClient.getPreviousID();
                String previousNodeIP = NamingClient.getIPAddress(previousNodeID);

                // Retransfer the file and its log to the previous node
                retransmitFile(jo, previousNodeIP);
                return 0;
            } else {
                // Store the replicated file
                FileOutputStream os_file = new FileOutputStream(file_path);
                os_file.write(data.getBytes());
                os_file.close();

                // Store the log of the replicated file
                os_file = new FileOutputStream(log_file_path);
                String update_text = date + " - Change of owner caused by shutdown.\n";
                os_file.write((log_data + update_text).getBytes());
                os_file.close();
            }
        } else if (Objects.equals(extra_message, "ENTRY_CREATE")) {
            // Store the replicated file
            FileOutputStream os_file = new FileOutputStream(file_path);
            os_file.write(data.getBytes());
            os_file.close();

            // Create a log for the file
            os_file = new FileOutputStream(log_file_path);
            String new_text = date + " - File is added & receives first owner.\n";
            os_file.write(new_text.getBytes());
            os_file.close();
        } else if (Objects.equals(extra_message, "ENTRY_MODIFY")) {
            // Store the replicated file
            FileOutputStream os_file = new FileOutputStream(file_path);
            os_file.write(data.getBytes());
            os_file.close();

            // Update the log
            os_file = new FileOutputStream(log_file_path, true);
            String update_text = date + " - Modification happened.\n";
            os_file.write(update_text.getBytes());
            os_file.close();
        } else if (Objects.equals(extra_message, "ENTRY_DELETE")) {
            Files.deleteIfExists(Path.of(file_path));
            Files.deleteIfExists(Path.of(log_file_path));
        } else if (Objects.equals(extra_message, "OVERFLOW")) {
            System.out.println("ERROR - Overflow received when watching for events in the local_files directory!");
            DiscoveryClient.failure();
        }
        return 0;
    }
        //if (Objects.equals(extra_message, "warning")) {
        //    System.out.println("I am the owner of " + fileName + " and got a warning.");
        //    if (wasDownloaded(file_path)) {
        //        // Update the log file with the download locations
        //        System.out.println(fileName + " contains a download of the file, update the log file");
//
        //        FileOutputStream os_log;
        //        try {
        //            os_log = new FileOutputStream(log_path.toString() + '\\' + fileName + ".log", true);
        //        } catch (FileNotFoundException e) {
        //            System.out.println("Log file not found!");
        //            System.out.println("\tLooking for name "+fileName+ ".log using the method get('name') failed!");
        //            System.out.println("\tCheck if 'name' is the right key in the object: " + jo);
        //            System.out.println("\n\tException:\n\t"+e.getMessage());
        //            return -2;
        //        }
//
        //        try {
        //            // Get current timestamp
        //            String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(System.currentTimeMillis()));
//
        //            // Create the content of the file
        //            String update_text = "Update of this log file happened due to change of owner at: " + date + "\n";
//
        //            // Write the text that indicates that the log is update
        //            os_log.write(update_text.getBytes());
//
        //            // Append the lines of the old log file
        //            os_log.write(message.getPayload());
//
        //            // Close the output stream
        //            os_log.close();
        //        } catch (IOException e) {
        //            System.out.println("Failed to write to log file "+fileName+".log!");
        //            System.out.println("\n\tException:\n\t"+e.getMessage());
        //            return -2;
        //        }
        //    } else {
        //        // This log file can be removed since the file was never downloaded
        //        System.out.println(fileName + " contains no download of the file, remove the log file");
        //        Files.deleteIfExists(Path.of(file_path));
        //    }
//
        //    return 0;
        //}
//
        //FileOutputStream os_file;
        //try {
        //    os_file = new FileOutputStream(replicated_file_path.toString() + '\\' + fileName);
        //} catch (FileNotFoundException e) {
        //    System.out.println("File not found!");
        //    System.out.println("\tLooking for name "+fileName+ " using the method get('name') failed!");
        //    System.out.println("\tCheck if 'name' is the right key in the object: " + jo);
        //    System.out.println("\n\tException:\n\t"+e.getMessage());
        //    return -1;
        //}
//
        //try {
        //    os_file.write(message.getPayload());
        //    os_file.close();
        //} catch (IOException e) {
        //    System.out.println("Failed to write to file "+fileName+"!");
        //    System.out.println("\n\tException:\n\t"+e.getMessage());
        //    return -1;
        //}
//
        //FileOutputStream os_log;
        //try {
        //    os_log = new FileOutputStream(log_path.toString() + '\\' + fileName + ".log", true);
        //} catch (FileNotFoundException e) {
        //    System.out.println("Log file not found!");
        //    System.out.println("\tLooking for name "+fileName+ ".log using the method get('name') failed!");
        //    System.out.println("\tCheck if 'name' is the right key in the object: " + jo);
        //    System.out.println("\n\tException:\n\t"+e.getMessage());
        //    return -2;
        //}
//
        //try {
        //    // Get current timestamp
        //    Date date = new Date(System.currentTimeMillis());
        //    String formatted_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
//
        //    // Create the content of the file
        //    String text = "Node " + nodeName + " with IP " + IPAddress + " became the file owner on: " + formatted_date + "\n";
//
        //    // Write the file
        //    os_log.write(text.getBytes());
        //    os_log.close();
        //} catch (IOException e) {
        //    System.out.println("Failed to write to log file "+fileName+".log!");
        //    System.out.println("\n\tException:\n\t"+e.getMessage());
        //    return -2;
        //}
//
        //return 0;

    public boolean fileStoredLocally(String file_name) {
        // Test if the file is locally stored -> send it to the previous node
        File[] localFiles = new File(local_file_path.toString()).listFiles();

        // Test if the local directory is found
        if (localFiles == null) {
            System.out.println("ERROR - The local directory cannot be found!");
            DiscoveryClient.failure();
        }

        // Loop through the files and search for the received file name
        for (File file : localFiles) {
            // Get info of the file
            String localFileName = file.getName();

            // Test if this the file we were looking for
            if (localFileName.equals(file_name)) {
                return true;
            }
        }
        return false;
    }
}
