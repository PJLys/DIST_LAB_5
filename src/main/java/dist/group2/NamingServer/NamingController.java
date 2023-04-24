package dist.group2.NamingServer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping(path = {"api/naming"})
public class NamingController {
    private final NamingService service;

    @Autowired
    public NamingController(NamingService service) {
        this.service = service;
    }

    @PostMapping
    public void addNode(@RequestBody Map<String, String> node) {
        this.service.addNode(node);
    }

    @DeleteMapping(path = {"{nodeName}"})
    public void deleteNode(@PathVariable("nodeName") String nodeName) {
        this.service.deleteNode(nodeName);
    }

    @GetMapping
    public String findFile(String fileName) {
        return this.service.findFile(fileName);
    }

    @GetMapping(path = {"/translate"})
    public String getIPAddress(int nodeID) {
        return this.service.getIPAddress(nodeID);
    }
}
