package dist.group2;


public class ReplicationClient {

    private Path folder_path = Path.of(new File("").getAbsolutePath().concat("\\src\\files"));
    //Stores the local files that need to be replicated
    private WatchService file_daemon = FileSystems.getDefault().newWatchService();


}
