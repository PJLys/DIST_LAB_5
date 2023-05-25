package dist.group2;


import net.minidev.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

@RestController
@RequestMapping(path="api/node")
public class ReplicationController {
    private final ReplicationClient client;

    @Autowired  // Dependency injection, service will be automatically instantiated and injected into the constructor
    public ReplicationController(ReplicationClient client) {
        this.client = client;
    }

    @PostMapping
    public void replicateFile(@RequestBody JSONObject fileMessage) throws IOException {
        client.replicateFile(fileMessage);
    }
}
