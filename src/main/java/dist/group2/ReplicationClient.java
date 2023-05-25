package dist.group2;

import jakarta.annotation.PreDestroy;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.hibernate.cfg.NotYetImplementedException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.ip.udp.UnicastReceivingChannelAdapter;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.rmi.UnexpectedException;
import java.text.SimpleDateFormat;
import java.util.*;


@Service
public class ReplicationClient implements Runnable{
    private String nodeName = InetAddress.getLocalHost().getHostName();
    private int nodeID = DiscoveryClient.hashValue(nodeName);
    private String IPAddress = InetAddress.getLocalHost().getHostAddress();
    WatchService file_daemon = FileSystems.getDefault().newWatchService();
    private final Path local_file_path = Path.of(new File("").getAbsolutePath().concat("/src/local_files"));  //Stores the local files that need to be replicated
    private final Path replicated_file_path = Path.of(new File("").getAbsolutePath().concat("/src/replicated_files"));  //Stores the local files that need to be replicated
    private final Path log_path = Path.of(new File("").getAbsolutePath().concat("/src/log_files"));  //Stores the local files that need to be replicated
    private static ReplicationClient client=null;

    private ReplicationClient() throws IOException {}

    public static ReplicationClient getInstance() throws IOException {
        if (client == null) {
            client = new ReplicationClient();
        }
        return client;
    }

    public void createDirectories() throws IOException {
        createDirectory(local_file_path);
        createDirectory(replicated_file_path);
        createDirectory(log_path);
    }

    public void createDirectory(Path path) throws IOException {
        File directory = new File(path.toString());

        if (!directory.exists()) {
            boolean success = directory.mkdir();
            if (success) {
                System.out.println("Created directory " + directory);
            } else {
                System.out.println("ERROR - can't create directory " + directory);
            }
        } else {
            System.out.println("Directory " + directory + " already exists");
            removeDirectory(path);
        }
    }

    public void setFileDirectoryWatchDog() {
        try {
            this.local_file_path.register(file_daemon,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE);
        } catch (Exception e) {
            System.out.println("ERROR - Failed to set watchdog for directory\n");
            System.out.println("\tError Message: " + e.getMessage());
            System.out.println("\tError Stack Trace: " + Arrays.toString(e.getStackTrace()));
        }
    }

    /**
     * This method is used when an event is detected
     * @param event detected WatchEvent
     * @return error code
     */
    public int event_handler(WatchEvent<?> event) {
        try {
            Path filename = (Path) event.context();
            String filePath = local_file_path.toString() + '/' + filename;

            if (filePath.endsWith(".swp")) {
                return 0;
            }

            System.out.println("Update of file detected in file with path: " + filePath + ", sending notice to owner");
            String replicator_loc = NamingClient.findFile(Path.of(filePath).getFileName().toString());
            sendFileToNode(filePath, null, replicator_loc, event.kind().toString());
        } catch (IOException e) {
            System.out.println("ERROR - Failed to send file!");
            System.out.println("\tError Message: " + e.getMessage());
            System.out.println("\tError Stack Trace: " + Arrays.toString(e.getStackTrace()));
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

    public static void removeDirectory(Path path) throws IOException {
        File directory = new File(path.toString());
        File[] files = directory.listFiles();

        assert files != null;
        for (File file : files) {
            Files.deleteIfExists(Path.of(directory + "/" + file.getName()));
        }
    }

    // Create files to store on this node
    public void addFiles() throws IOException {
        String name = InetAddress.getLocalHost().getHostName();
        // Create 3 file names to add
        ArrayList<String> fileNames = new ArrayList<>();
        fileNames.add("1_" + name);
        fileNames.add("2_" + name);
        fileNames.add("3_" + name);

        // Create the files
        String str = "Text";
        BufferedWriter writer;
        for (String fileName : fileNames) {
            System.out.println("Added file: " + local_file_path + "/" + fileName);
            writer = new BufferedWriter(new FileWriter(local_file_path + "/" + fileName));
            writer.write(str);
            writer.flush();
            writer.close();
        }
    }

    public void replicateFiles() throws IOException {
        System.out.println("Replicate local files");
        File folder = new File(local_file_path.toString());
        File[] files = folder.listFiles();

        assert files != null;
        for (File file : files) {
            System.out.println("Replicating file: " + file.toString());
            if (file.isFile()) {
                String fileName = file.getName();
                String filePath = local_file_path.toString() + '/' + fileName;
                String replicator_loc = NamingClient.findFile(Path.of(filePath).getFileName().toString());
                System.out.println("Send file " + file + " to " + replicator_loc);
                sendFileToNode( filePath, null, replicator_loc, "ENTRY_CREATE");
            }
        }
    }

    public void changeOwnerWhenNodeIsAdded() throws IOException, InterruptedException {
        System.out.println("Node is added to the network, check if files need to change owner");
        File folder = new File(replicated_file_path.toString());
        File[] files = folder.listFiles();

        assert files != null;
        for (File file : files) {
            if (file.isFile()) {
                String file_name = file.getName();
                String file_owner = NamingClient.findFile(file_name);

                if (Objects.equals(IPAddress, file_owner)) {
                    // The new node does not become the new owner of the file
                } else {
                    if (file_name.endsWith(".swp")) {
                        return;
                    }

                    System.out.println("Change in file owner after new node joined. Send file " + file_name + " to its new owner " + file_owner);

                    // Replicate the file to the new owner
                    String file_path = replicated_file_path.toString() + '/' + file_name;
                    String log_file_path = log_path.toString() + '/' + file_name + ".log";;
                    sendFileToNode(file_path, log_file_path, file_owner, "ENTRY_CREATE");

                    // Delete file on this node
                    Files.deleteIfExists(Path.of(file_path));
                    Files.deleteIfExists(Path.of(log_file_path));
                }
            }
        }
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
        assert localFiles != null;
        for (File file : localFiles) {
            // Get info of the file
            String fileName = file.getName();
            System.out.println("FILENAME " + fileName);
            String filePath = local_file_path.toString() + '/' + fileName;

            // The destination is the owner of the file instead of the previous node
            String destinationIP = NamingClient.findFile(fileName);

            System.out.println("Send warning to delete file " + file.getName() + " to node " + destinationIP);

            // Warn the owner of the file to delete the replicated file
            sendFileToNode(filePath, null, destinationIP, "ENTRY_DELETE");
            if (!Objects.equals(destinationIP, IPAddress)) {
                sleep(300);
            }
        }

        // Send the replicated files and their logs to the previous node which will become the new owner of the file.
        // When the previous node already stores this file locally -> send it to its previous node
        for (File file : replicatedFiles) {
            System.out.println("Replicating file " + file.getName() + " to node " + previousNodeIP);

            // Get info of the file
            String fileName = file.getName();
            String filePath = replicated_file_path.toString() + '/' + fileName;
            String logPath = log_path.toString() + '/' + fileName + ".log";

            // Transfer the file and its log to the previous node
            sendFileToNode(filePath, logPath, previousNodeIP, "ENTRY_SHUTDOWN_REPLICATE");
            if (!Objects.equals(previousNodeIP, IPAddress)) {
                sleep(300);
            }
        }
    }

    public void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch (Exception e) {
            System.out.println("ERROR - Sleep failed");
            System.out.println("\tError Message: " + e.getMessage());
            System.out.println("\tError Stack Trace: " + Arrays.toString(e.getStackTrace()));
        }
    }

    public void sendFileToNode(String filePath, String logPath, String nodeIP, String extra_message) throws IOException {
        if (filePath.endsWith(".swp")) {
            return;
        }

        // Create JSON object from File
        JSONObject jo = new JSONObject();

        // Get the info of the file
        Path fileLocation = Path.of(filePath);
        String fileName = fileLocation.getFileName().toString();

        // Put the payload data in the JSON object
        jo.put("name", fileName);
        jo.put("extra_message", extra_message);

        // Only send data if the file still exists
        if (Objects.equals(extra_message, "ENTRY_DELETE")) {
            jo.put("data", null);
        } else {
            jo.put("data", Arrays.toString(Files.readAllBytes(fileLocation)));
        }

        // Also include the data of the log file when necessary
        if (logPath == null) {
            jo.put("log_data", "null");
        } else {
            jo.put("log_data", Arrays.toString(Files.readAllBytes(Path.of(logPath))));
        }

        updateFile(jo, nodeIP);
    }

    public void updateFile(JSONObject json, String nodeIP) throws IOException {
        if (Objects.equals(nodeIP, IPAddress)) {
            System.out.println("Send replicated version of file " + json.get("name") + " with action " + json.get("extra_message") + " to itself");
            implementUpdate(json);
        } else {
            System.out.println("Send replicated version of file " + json.get("name") + " with action " + json.get("extra_message") + " to node with IP " + nodeIP);
            transmitFileAsJSON(json, nodeIP);
        }
    }

    public void transmitFileAsJSON(JSONObject json, String nodeIP) {
        String url = "http://" + nodeIP + ":" + 8082 + "/api/node";
        RestTemplate restTemplate = new RestTemplate();

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("name", json.get("name"));
        requestBody.put("extra_message", json.get("extra_message"));
        requestBody.put("data", json.get("data"));
        requestBody.put("log_data", json.get("log_data"));

        // Specify media type
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);

        try {
            restTemplate.postForObject(url, requestEntity, Void.class);
        } catch (Exception e) {
            System.out.println("ERROR - posting file throws IOException");
            System.out.println("\tError Message: " + e.getMessage());
            System.out.println("\tError Stack Trace: " + Arrays.toString(e.getStackTrace()));
        }
    }

    public boolean fileStoredLocally(String file_name) {
        // Test if the file is locally stored -> send it to the previous node
        File[] localFiles = new File(local_file_path.toString()).listFiles();

        // Test if the local directory is found
        if (localFiles == null) {
            System.out.println("ERROR - The local directory cannot be found!");
            DiscoveryClient.failure();
        }

        // Loop through the files and search for the received file name
        assert localFiles != null;
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

    // POST file using REST
    public void replicateFile(JSONObject json) throws IOException {
        System.out.println("Received file " + json.get("name") + " using REST with action " + json.get("extra_message"));
        implementUpdate(json);
    }

    public void implementUpdate(JSONObject json) throws IOException {
        String file_name = (String) json.get("name");
        String extra_message = (String) json.get("extra_message");
        String data = (String) json.get("data");
        String file_path = replicated_file_path.toString() + '/' + file_name;
        String log_file_path = log_path.toString() + '/' + file_name + ".log";

        System.out.println("Implement update " + extra_message + " of file " + file_name);

        // Get current timestamp
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(System.currentTimeMillis()));

        if (Objects.equals(extra_message, "ENTRY_SHUTDOWN_REPLICATE")) {
            boolean fileFoundLocally = fileStoredLocally(file_name);

            if (fileFoundLocally) {
                // Find the IP address of the previous node
                int previousNodeID = DiscoveryClient.getPreviousID();
                String previousNodeIP = NamingClient.getIPAddress(previousNodeID);

                // Retransfer the file and its log to the previous node
                updateFile(json, previousNodeIP);
            } else {
                // Store the replicated file
                FileOutputStream os_file = new FileOutputStream(file_path);
                os_file.write(data.getBytes());
                os_file.close();

                // Store the log of the replicated file
                os_file = new FileOutputStream(log_file_path);
                String update_text = date + " - Change of owner caused by shutdown.\n";
                String log_data = (String) json.get("log_data");
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
    }
}
